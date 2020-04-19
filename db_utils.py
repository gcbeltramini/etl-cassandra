import csv
import glob
import os
import re
from typing import Callable, List, Tuple

from cassandra.cluster import Cluster, Session

CASSANDRA_KEYSPACE = 'udacity'
HEADER = ('artist', 'userFirstName', 'userGender', 'itemInSession',
          'userLastName', 'songLength', 'level', 'userLocation', 'sessionId',
          'songTitle', 'userId')  # "level": paid or free


def get_files(parent_dir: str, extension: str = 'csv') -> List[str]:
    """
    Create a list of files under `parent_dir` (and possibly under
    subfolders), with a specific extension.

    Parameters
    ----------
    parent_dir : str
        Parent directory.
    extension : str, optional
        File extension.

    Returns
    -------
    list[str]
        List of full file name.
    """
    file_path = []

    for root, _, _ in os.walk(parent_dir):
        file_path.extend(glob.glob(os.path.join(root,
                                                f'*.{extension:s}')))
    return sorted(file_path)


def read_all_rows(csv_files: List[str]) -> List[List[str]]:
    """
    Concatenate all rows from all files, excluding the header (first row).

    Parameters
    ----------
    csv_files : list[str]
        CSV file names.

    Returns
    -------
    list[list[str]]
        List of rows of all files. Each element is the list of columns of that
        row.
    """
    rows_list = []

    for f in csv_files:

        # Read CSV file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)  # skip header

            # Extract each data row one by one
            for line in csvreader:
                rows_list.append(line)

    return rows_list


def read_header(csv_filename: str) -> List[str]:
    """
    Read only header from CSV file.

    Parameters
    ----------
    csv_filename : str
        CSV file name.

    Returns
    -------
    list[str]
        Header.
    """
    with open(csv_filename, 'r', encoding='utf8', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        return next(csvreader)


def write_csv(rows: List[str],
              csv_filename: str,
              header: Tuple[str] = HEADER) -> None:
    """
    Create CSV file with data from `rows`.

    Parameters
    ----------
    rows: list[str]
        Rows to write to `csv_filename`.
    csv_filename : str
        File name of the new CSV file.
    header : tuple[str]
        Column names.

    Returns
    -------
    None
        File `csv_filename` will be created with the content from `rows`.
    """
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL,
                         skipinitialspace=True)

    with open(csv_filename, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')

        # Write header
        writer.writerow(header)

        # Write row by row
        for row in rows:
            if row[0] == '':  # skip row when there is no artist
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6],
                             row[7], row[8], row[12], row[13], row[16]))


def create_insert_command(table_name: str, table_info: str) -> str:
    """
    Create SQL INSERT command.

    Parameters
    ----------
    table_name : str
        Table name.
    table_info : str
        Table information passed to SQL CREATE TABLE command.

    Returns
    -------
    str
        SQL INSERT command.
    """
    columns = re.findall('(\w+) (?:INT|FLOAT|TEXT)', table_info)
    return ('INSERT INTO {table:s} ({columns:s})\nVALUES ({string:s});'
            ''.format(table=table_name,
                      columns=', '.join(columns),
                      string=', '.join(['%s' for _ in range(len(columns))])))


def cassandra_connect(keyspace: str = CASSANDRA_KEYSPACE
                      ) -> Tuple[Cluster, Session]:
    """
    Connect to Apache Cassandra cluster.

    Parameters
    ----------
    keyspace : str, optional
        Default keyspace to set for all queries made through the session.

    Returns
    -------
    cassandra.cluster.Cluster, cassandra.cluster.Session
    """

    # Make a connection to a Cassandra instance on the local machine
    cluster = Cluster(['127.0.0.1'])

    # Create session to establish connection and begin executing queries
    session = cluster.connect()

    # Create keyspace
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace:s}\n"
                    "WITH REPLICATION = { 'class' : 'SimpleStrategy', "
                    "'replication_factor' : 1 }")

    # Set the default keyspace for all queries made through this Session
    session.set_keyspace(keyspace)

    return cluster, session


def drop_table(table: str, session: Session) -> None:
    """
    Drop table.

    Parameters
    ----------
    table : str
        Table name.
    session : cassandra.cluster.Session
        Cassandra session.
    """
    session.execute(f'DROP TABLE IF EXISTS {table:s};')


def create_table(table_name: str, table_info: str, session: Session) -> None:
    """
    Drop and create table.

    Parameters
    ----------
    table_name : str
        Table name.
    table_info : str
        Table information passed to SQL CREATE TABLE command.
    session : cassandra.cluster.Session
        Cassandra session.
    """
    drop_table(table_name, session)
    create_command = f'CREATE TABLE IF NOT EXISTS {table_name:s}\n{table_info:s};'
    session.execute(create_command)


def insert_rows(table_name: str, table_info: str,
                row_fn: Callable[[tuple], tuple],
                csv_file: str, session: Session) -> None:
    """
    Insert rows from CSV file into table.

    Parameters
    ----------
    table_name : str
        Table name.
    table_info : str
        Table information passed to SQL CREATE TABLE command.
    row_fn : callable[tuple, tuple]
    csv_file : str
    session : cassandra.cluster.Session
        Cassandra session.
    """
    query = create_insert_command(table_name, table_info)
    with open(csv_file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(query, row_fn(line))


def run_query(query: str, session: Session) -> None:
    """
    Run SQL query and print rows.

    Parameters
    ----------
    query : str
        SQL query.
    session : cassandra.cluster.Session
        Cassandra session.
    """
    rows = session.execute(query)
    for row in rows:
        print(row)
