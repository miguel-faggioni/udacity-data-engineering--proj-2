#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This file allows running the needed SELECT queries on the Cassandra cluster,
using predefined SELECT statements that follow the PRIMARY KEYs of each table.

Example:
    $ python selects.py
"""
import cassandra
from cassandra.cluster import Cluster
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


def session_select(session,session_id,item_in_session):
    """Function that runs the SELECT query on the session history table

    Args:
        session_id (int): id of the session to select
        item_in_session (int): number of the item on the session selected

    Returns:
        (cassandra.cluster.ResultSet): rows returned from the SELECT query
    """
    return session.execute(session_table_select,(session_id,item_in_session))

def user_select(session,user_id,session_id):
    """Function that runs the SELECT query on the user history table

    Args:
        user_id (int): id of the user to select
        session_id (int): id of the session to select

    Returns:
        (cassandra.cluster.ResultSet): rows returned from the SELECT query
    """
    return session.execute(user_table_select,(user_id,session_id))

def songplay_select(session,song_name):
    """Function that runs the SELECT query on the user history table

    The string formating is done outside the `session.execute` function
    since the parameter consists of only one element and the parsing gets
    confused trying to interpret it as a tuple otherwise.

    Args:
        song_name (str): name of the song to select

    Returns:
        (cassandra.cluster.ResultSet): rows returned from the SELECT query
    """
    return session.execute(songplay_table_select%song_name)

def main():
    """Main function to test the SELECT queries

    Selects the artist, song title and song's length in the session history table
    that was heard during sessionId = 338, and itemInSession = 4.

    Selects the name of artist, song and user (first and last name) for userid = 10, 
    sessionid = 182; sorted by itemInSession.

    Selects every user (first and last name) in the songplay history table who
    listened to the song 'All Hands Against His Own'.
    """
    # create the connection
    cluster, session = connect_cassandra()
    # run the select on the session history table
    print('session history table SELECT:')
    try:
        rows = session_select(session,338,4)
    except Exception as e:
        print(e)
    for row in rows:
        print(row.artist_name,row.song_name,row.song_length)
    # run the select on the user history table
    print('\nuser history table SELECT:')
    try:
        rows = user_select(session,10,182)
    except Exception as e:
        print(e)
    for row in rows:
        print(row.item_in_session, row.artist_name,row.song_name,row.user_first_name,row.user_last_name)
    # run the select on the songplay history table
    print('\nsongplay history table SELECT:')
    try:
        rows = songplay_select(session,"'All Hands Against His Own'")
    except Exception as e:
        print(e)
    for row in rows:
        print(row.user_first_name,row.user_last_name)
    # close the connection
    session.shutdown()
    cluster.shutdown()

if __name__ == '__main__':
    main()
