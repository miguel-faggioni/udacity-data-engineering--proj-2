#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ETL pipeline for the Data Modeling with Apache Cassandra project for Udacity.

This file allows parsing of .csv files inside a folder and subsequent insertion
into 3 tables on a Cassandra cluster running locally.

Example:
    $ python etl.py
"""

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

csv_folder = '/event_data'
"""str: Folder in which the .csv files to be inserted are stored.
"""

csv_file = 'event_datafile_new.csv'
"""str: File where the .csv files from the `csv_folder` will be saved
after being united.
"""

def get_files_from_dir(dirpath):
    """Function that lists all files inside the given `dirpath` relative
    to the current working directory

    Args:
        dirpath (str): Path to the folder

    Returns:
        list: List of the files contained in the `dirpath`
    """    

    # get the path relative to current path
    filepath = os.getcwd() + dirpath
    # iterate over the file list
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
    # return the list with all files
    return file_path_list

def merge_csvs(file_list,output_csv):
    """Function that merges a list of .csv files into one .csv file containing 
    all rows of the files on `file_list`

    Args:
        file_list (list): List of .csv files to merge
        output_csv (str): Name of the .csv file in which to save the output
    """
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
    # open the `output_csv` file for writing
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
    """Function that connects to the Cassandra Cluster, creates the needed keyspace if 
    it still doesn't exists, and connects to it

    Returns:
        (cassandra.cluster.Cluster): Cluster connection, used for later shutdown
        (cassandra.cluster.Session): Cassandra session, used to run queries
    """
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
    """Function that runs a list of queries in the received Cassandra session

    Args:
        session (cassandra.cluster.Session): session in which the queries will be run
        query_list (list): list of queries to be run
    """
    for query in query_list:
        try:
            session.execute(query)
        except Exception as e:
            print('Error running query [{}]'.format(query))
            print(e)
            return

def insert_rows(session,csv_file):
    """Function that inserts the rows of `csv_file` into the session, user, and song tables
    in the Cassandra cluster. The current progress is printed after each row is successfully
    inserted into the tables.

    Args:
        session (cassandra.cluster.Session): session in which to run the insert queries
        csv_file (str): name of the .csv file to be read and inserted
    """
    
    # open csv file
    df = pd.read_csv(csv_file)

    # for each row on the file
    for i, row in df.iterrows():
        # insert into session history table
        try:
            session.execute(
                session_table_insert,
                tuple(row[[
                    'sessionId',
                    'itemInSession',
                    'artist',
                    'song',
                    'length',
                    'userId',
                    'firstName',
                    'lastName',
                    'gender',
                    'level',
                    'location'
                ]])
            )
        except Exception as e:
            print("Error inserting into session table")
            print(e)
            return
        # insert into user history table
        try:
            session.execute(
                user_table_insert,
                tuple(row[[
                    'userId',
                    'sessionId',
                    'itemInSession',
                    'artist',
                    'song',
                    'length',
                    'firstName',
                    'lastName',
                    'gender',
                    'level',
                    'location'
                ]])
            )
        except Exception as e:
            print("Error inserting into user table")
            print(e)
            return
        # insert into songplay history table
        try:
            session.execute(
                songplay_table_insert,
                tuple(row[[
                    'song',
                    'userId',
                    'artist',
                    'length',
                    'sessionId',
                    'itemInSession',
                    'firstName',
                    'lastName',
                    'gender',
                    'level',
                    'location'
                ]])
            )
        except Exception as e:
            print("Error inserting into song table")
            print(e)
            return
        # print how far along we are
        print('{}/{} rows inserted'.format(i,len(df)))

def main():
    """Main function of the ETL pipeline.

    Merges all the files on `csv_folder` into the `csv_file`; 
    creates a connection to the Cassandra cluster;
    creates the tables if they still don't exists;
    inserts the rows into each table;
    then shuts down the connection.
    """
    # get all the files in the event_data folder
    file_list = get_files_from_dir(csv_folder)
    # merge all files into one csv
    merge_csvs(file_list,csv_file)
    # create the connection
    cluster, session = connect_cassandra()
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
