#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv
from cql_queries import *

def get_files_from_dir(dirpath):
    # get the path relative to current path
    filepath = os.getcwd() + dirpath
    # iterate over the file list
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
    # return the list with all files
    return file_path_list

def merge_csvs(file_list,output_csv):
    # initiating an empty list of rows that will be generated from each file
    merged_rows = [] 
    
    # for every filepath in the file list 
    for f in file_list:

        # read csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # create a csv reader object 
            csvreader = csv.reader(csvfile)
            # skip the header
            next(csvreader)
        
            # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                merged_rows.append(line) 
            
    # uncomment the code below if you would like to get total number of rows 
    #print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    #print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(output_csv, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow([
            'artist',
            'firstName',
            'gender',
            'itemInSession',
            'lastName',
            'length',
            'level',
            'location',
            'sessionId',
            'song',
            'userId'
        ])
        for row in merged_rows:
            # skip empty rows
            if (row[0] == ''):
                continue
            # otherwise write to the merged csv file
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

def connect_cassandra():
    # connect to the cluster
    try: 
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
    except Exception as e:
        print(e)
    # create the keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )
    except Exception as e:
        print(e)
    # connect to the keyspace
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)
    return (cluster,session)

def run_queries(session,query_list):
    for query in query_list:
        try:
            session.execute(query)
        except Exception as e:
            print('Error running query [{}]'.format(query))
            print(e)
            return

def insert_rows(session,csv_file):
    # open csv file
    df = pd.read_csv(csv_file)

    # for each row on the file
    for i, row in df.iterrows():
        # reorder columns to match inserts
        info = tuple(row[['artist','song','length','sessionId','itemInSession','userId','firstName','lastName','gender','level','location']])
        # insert into session table
        try:
            session.execute(session_table_insert, info)
        except Exception as e:
            print("Error inserting into session table")
            print(e)
            return
        # insert into user table
        try:
            session.execute(user_table_insert, info)
        except Exception as e:
            print("Error inserting into user table")
            print(e)
            return
        # insert into song table
        try:
            session.execute(song_table_insert, info)
        except Exception as e:
            print("Error inserting into song table")
            print(e)
            return
        # print how far along we are
        print('{}/{} rows inserted'.format(i,len(df)))

def main():
    csv_folder = '/event_data'
    csv_file = 'event_datafile_new.csv'
                
    # get all the files in the event_data folder
    file_list = get_files_from_dir(csv_folder)
    # merge all files into one csv
    merge_csvs(file_list,csv_file)
    # create the connection
    cluster, session = connect_cassandra()
    # drop the old tables
    run_queries(session,drop_table_queries)
    print('Tables successfully dropped')
    # create the needed tables
    run_queries(session,create_table_queries)
    print('Tables successfully created')
    # insert in the tables
    insert_rows(session,csv_file)
    print('All rows successfully created')
    # close the connection
    session.shutdown()
    cluster.shutdown()

if __name__ == '__main__':
    main()
