- [Data Modeling with Apache Cassandra - Udacity](#org7dbecfc)
  - [Introduction](#org5accfb6)
  - [Project Overview](#org9c32949)
- [Folder structure](#orgc175d46)
- [Usage](#org148e043)
  - [Removing old tables](#orgc54a648)
  - [Running the pipeline](#org12a2a03)
  - [Selecting from the tables](#org0115b1e)
- [Motivation for the Primary keys and Clustering Columns](#org557d7f5)
  - [Session history table](#org08da6f0)
  - [User history table](#orgf280072)
  - [Songplay history table](#orgd721c7b)


<a id="org7dbecfc"></a>

# Data Modeling with Apache Cassandra - Udacity

This repository is intended for the second project of the Udacity Data Engineering Nanodegree Program: Data Modeling with Apache Cassandra.

The introduction and project description were taken from the Udacity curriculum, since they summarize the activity better than I could.


<a id="org5accfb6"></a>

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.


<a id="org9c32949"></a>

## Project Overview

In this project, you'll apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, you will need to model your data by creating tables in Apache Cassandra to run queries. You are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

We have provided you with a project template that takes care of all the imports and provides a structure for ETL pipeline you'd need to process this data.


<a id="orgc175d46"></a>

# Folder structure

```
/
├── images - contains the images for the `Project_1B_ Project_Template.ipynb` notebok
├── event_data - contains the event data in .csv format
│   ├── (...)
│   └── <year>-<month>-<day>-events.csv
├── event_datafile_new.csv - .csv file with the content of the .csv files on ./event_data/
├── reset.py - contains code that drops the tables for a fresh start
├── etl.py - code to merge the files on ./event_data/ into ./event_datafile_new.csv, create the needed tables and insert the data into said tables
├── Project_1B_ Project_Template.ipynb - jupyter notebook to walk through the necessary steps to run the ETL pipeline
├── selects.py - code run the needed SELECT queries
├── cql_queries.py - file containing the CQL queries to select from, create, and drop the tables needed
├── README.md - this file in markdown
└── README.org - this file in orgmode
```


<a id="org148e043"></a>

# Usage


<a id="orgc54a648"></a>

## Removing old tables

To remove the tables and guarantee a fresh start when testing the ETL pipeline, run the following command in the terminal at the root of the project.

```bash
python reset.py
```

It connects to the Apache Cassandra cluster running locally, creates the keyspace if it still doesn't exist and connects to it, then drops the tables.


<a id="org12a2a03"></a>

## Running the pipeline

The `etl.py` file will run the ETL pipeline according to the project specifications. To run it, run the following command in the terminal at the root of the project.

```bash
python etl.py
```

It will:

1.  merge the .csv files on `./event_data/` into `./event_datafile_new.csv`;
2.  connect to the Apache Cassandra cluster running on localhost;
3.  create the tables needed for the pipeline if they don't exist yet;
4.  parse the merged .csv file, inserting each row on each of the 3 tables;
5.  then finally closing the connection to the cluster.


<a id="org0115b1e"></a>

## Selecting from the tables

In order to run the SELECT statements described in the project specifications, run the following command in the terminal at the root of the project.

```bash
python selects.py
```

It will run the SELECT statements as required (also present in the jupyter notebook), but unlike the jupyter notebook, it will run a `SELECT *` for easier checking of the returned rows. The printed values will follow the return values specified, but running the functions outside `main()` allows the user to get info beyond the needed for the project specification.


<a id="org557d7f5"></a>

# Motivation for the Primary keys and Clustering Columns


<a id="org08da6f0"></a>

## Session history table

For the session history table, the columns `session_id`, and `item_in_session` were selected as Primary key, with the `item_in_session` being the Clustering column.

The columns were selected due to the query that was going to be run on the table, and as they were enough to guarantee no collision would happen on the event data, no other clustering column was included.


<a id="orgf280072"></a>

## User history table

For the user history table, the columns `user_id`, `session_id`, and `item_in_session` were selected as Primary key, with the `session_id` and `item_in_session` being Clustering columns.

The `item_in_session` was not in the WHERE part of the query that was going to be run, but adding it as a clustering column also allowed the returned rows to be sorted by it, as per the query specification. But even if using it to sort the results was not a requirement, it would not be possible to save multiple songs played in the same session by a user without it.


<a id="orgd721c7b"></a>

## Songplay history table

For the songplay history table, the columns `song_name`, `artist_name`, and `user_id` were selected as Primary key, with the `artist_name` and `user_id` being Clustering columns.

Since only the `song_name` was used in the WHERE part of the query, the other 2 columns were as Clustering columns to make sure it was possible to track songplays and not only existing songs.