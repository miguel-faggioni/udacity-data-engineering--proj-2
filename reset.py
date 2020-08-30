#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Python file to reset the tables needed for the ETL pipeline.

Ideally to be run before testing the ETL.

Example:
    $ python reset.py
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

def main():
    """Creates a connection to the Cassandra cluster;
    drops the tables if they exist;
    then shuts down the connection.
    """
    # create the connection
    cluster, session = connect_cassandra()
    # drop the old tables
    run_queries(session,drop_table_queries)
    print('Tables successfully dropped')
    # close the connection
    session.shutdown()
    cluster.shutdown()

if __name__ == '__main__':
    main()
