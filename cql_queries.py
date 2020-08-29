#!/usr/bin/env python
# -*- coding: utf-8 -*-

# table removal queries
session_table_drop = "DROP TABLE IF EXISTS session_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"

# table creation queries
session_table_create = """
CREATE TABLE IF NOT EXISTS session_table (
    artist_name text,
    song_name text,
    song_length decimal,
    session_id int,
    item_in_session int,
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
    artist_name text,
    song_name text,
    song_length decimal,
    session_id int,
    item_in_session int,
    user_id int,
    user_first_name text,
    user_last_name text,
    user_gender text,
    user_level text,
    user_location text,
    PRIMARY KEY (user_id,session_id,item_in_session)
);
"""
song_table_create = """
CREATE TABLE IF NOT EXISTS song_table (
    artist_name text,
    song_name text,
    song_length decimal,
    session_id int,
    item_in_session int,
    user_id int,
    user_first_name text,
    user_last_name text,
    user_gender text,
    user_level text,
    user_location text,
    PRIMARY KEY (song_name,artist_name)
);
"""

# data insertion queries
session_table_insert = """
INSERT INTO session_table (
    artist_name,
    song_name,
    song_length,
    session_id,
    item_in_session,
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
    artist_name,
    song_name,
    song_length,
    session_id,
    item_in_session,
    user_id,
    user_first_name,
    user_last_name,
    user_gender,
    user_level,
    user_location
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
song_table_insert = """
INSERT INTO song_table (
    artist_name,
    song_name,
    song_length,
    session_id,
    item_in_session,
    user_id,
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
song_table_select = """
SELECT user_first_name, user_last_name
FROM song_table
WHERE 
    song_name = %s
"""

# query lists
create_table_queries = [
    session_table_create,
    user_table_create,
    song_table_create
]
drop_table_queries = [
    session_table_drop,
    user_table_drop,
    song_table_drop    
]