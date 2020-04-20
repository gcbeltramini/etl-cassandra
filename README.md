# Data modeling with Apache Cassandra

## Introduction

A startup called `Sparkify` wants to analyze the data they've been collecting on songs and user
activity on their new music streaming app. The analysis team is particularly interested in
understanding what songs users are listening to. Currently, there is no easy way to query the data
to generate the results, since the data reside in a directory of CSV files on user activity on the
app.

They want an Apache Cassandra database against which they can run queries on song play data to
answer the questions. The queries are known, as described below.

## Project details

### Datasets

There is one dataset: `event_data`, a directory of CSV files partitioned by date. For example:

- `event_data/2018-11-08-events.csv`
- `event_data/2018-11-09-events.csv`

### Queries

1. Give me the artist, song title and song's length in the music app history that was heard during
`sessionId` = 338, and `itemInSession` = 4
1. Give me only the following: name of artist, song (sorted by `itemInSession`) and user (first and
last name) for `userid` = 10, `sessionid` = 182
1. Give me every user name (first and last) in my music app history who listened to the song "All
Hands Against His Own"

## Project structure

```text
├── README.md: this file
├── db_utils.py: functions for the ETL
├── etl-cassandra.ipynb: ETL pipeline (explained below)
├── event_data: folder with the CSV files
│   ├── 2018-11-01-events.csv
│   ├── (...)
│   └── 2018-11-30-events.csv
├── requirements.txt: project requirements (Python libraries)
├── requirements_dev.txt: additional Python requirements to run in the Jupyter notebook
├── requirements_test.txt: additional Python requirements to run unit tests
└── test_db_utils.py: unit tests
```

The file `etl-cassandra.ipynb` will:

- create a single CSV file in the current directory from all CSV files in `event_data` (this is the
denormalized dataset)
- create one table in Cassandra per question to address
  - The tables are designed to answer the required queries.
- insert the data into these tables
- run the queries
  - Since the CSV data is in memory, we can compare the output with the CSV content.
- drop all tables and close the connection

## Setup

### Install Cassandra

- In macOS with [Homebrew](https://brew.sh/): `brew install cassandra`; or
- In general, follow the instructions from <http://cassandra.apache.org/doc/latest/getting_started/installing.html>

Check the installation: `cassandra -v` (it should return the installed version).

### Start and stop Cassandra

- In macOS, if installed with [Homebrew](https://brew.sh/):
  - Start: `brew services start cassandra`
  - Stop: `brew services stop cassandra`
  - List running services: `brew services list`
  - See the logs:
    - `tail -f "$(brew --prefix)"/var/log/cassandra/debug.log` (debug level)
    - `tail -f "$(brew --prefix)"/var/log/cassandra/system.log` (info level)
- In general:
  - Start: `cassandra -f` (`-f` is for foreground, otherwise it runs like a system daemon)
  - Stop: simply abort the process above (Ctrl+C)
  - The logs will be in the terminal

### Python environment

Create a conda environment called `etl-env-cassandra` and install the requirements in it.

```shell
conda create -yn etl-env-cassandra python=3.7 -c defaults -c conda-forge --file requirements.txt
```

To use the new conda environment in Jupyter notebook:

```shell
conda install -yn base nb_conda_kernels
conda install -yn etl-env-cassandra --file requirements_dev.txt
conda activate base
jupyter notebook
```

Do not forget to stop the Cassandra service after you finish.

#### Run Python unit tests

```shell
conda install -yn etl-env-cassandra --file requirements_test.txt
conda activate etl-env-cassandra
pytest
```
