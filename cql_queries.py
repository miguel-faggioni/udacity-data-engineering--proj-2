#!/usr/bin/env python
# -*- coding: utf-8 -*-

# table removal queries
session_table_drop = "DROP TABLE IF EXISTS session_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"

# table creation queries
session_table_create = """
CREATE TABLE IF NOT EXISTS session_table (
    session_id int,
    item_in_session int,
    artist_name text,
    song_name text,
    song_length decimal,
    user_id int,
    user_first_name text,
    user_last_name text,
    user_gender text,
    user_level text,
    user_location text,
    PRIMARY KEY (session_id,item_in_session)
);
"""
user_table_create = """
CREATE TABLE IF NOT EXISTS user_table (
    user_id int,
    session_id int,
    item_in_session int,
    artist_name text,
    song_name text,
    song_length decimal,
    user_first_name text,
    user_last_name text,
    user_gender text,
    user_level text,
    user_location text,
    PRIMARY KEY (user_id,session_id,item_in_session)
);
"""
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay_table (
    song_name text,
    user_id int,
    artist_name text,
    song_length decimal,
    session_id int,
    item_in_session int,
    user_first_name text,
    user_last_name text,
    user_gender text,
    user_level text,
    user_location text,
    PRIMARY KEY (song_name,user_id)
);
"""

# data insertion queries
session_table_insert = """
INSERT INTO session_table (
    session_id,
    item_in_session,
    artist_name,
    song_name,
    song_length,
    user_id,
    user_first_name,
    user_last_name,
    user_gender,
    user_level,
    user_location
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
user_table_insert = """
INSERT INTO user_table (
    user_id,
    session_id,
    item_in_session,
    artist_name,
    song_name,
    song_length,
    user_first_name,
    user_last_name,
    user_gender,
    user_level,
    user_location
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
songplay_table_insert = """
INSERT INTO songplay_table (
    song_name,
    user_id,
    artist_name,
    song_length,
    session_id,
    item_in_session,
    user_first_name,
    user_last_name,
    user_gender,
    user_level,
    user_location
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

# data selection queries
session_table_select = """
SELECT artist_name, song_name, song_length
FROM session_table
WHERE 
    session_id = %s AND
    item_in_session = %s
"""
user_table_select = """
SELECT artist_name, song_name, user_first_name, user_last_name
FROM user_table
WHERE 
    user_id = %s AND
    session_id = %s
"""
songplay_table_select = """
SELECT user_first_name, user_last_name
FROM songplay_table
WHERE 
    song_name = %s
"""

# query lists
create_table_queries = [
    session_table_create,
    user_table_create,
    songplay_table_create
]
drop_table_queries = [
    session_table_drop,
    user_table_drop,
    songplay_table_drop    
]
