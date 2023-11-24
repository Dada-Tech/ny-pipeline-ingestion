#!/usr/bin/env python
# coding: utf-8

import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import os
import pyarrow.parquet as pq


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    is_parquet = params.parquet

    data_filename = f'output.{"parquet" if is_parquet else "csv"}'

    batch_size = 100000

    # download csv
    os.system(f'wget {url} -O {data_filename} --no-check-certificate')

    # SQL Alchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # get file iterator for parquet or csv file
    if is_parquet:
        parquet_file = pq.ParquetFile(data_filename)
        df_iter = parquet_file.iter_batches(batch_size=batch_size)

        # insert heading
        df = next(df_iter).to_pandas()
        insert_header(
            df=df, table_name=table_name, engine=engine, is_parquet=True)

        # insert all chunks iteratively
        while True:
            try:
                df = next(df_iter).to_pandas()
                insert_batch(df=df, table_name=table_name,
                             engine=engine, is_parquet=True)
            except StopIteration:
                print("Finished inserting all parquet batches.")
                break
    else:
        df_iter = pd.read_csv(
            data_filename, iterator=True, chunksize=batch_size)

        # insert heading
        df = next(df_iter)
        insert_header(df=df, table_name=table_name, engine=engine)

        # insert all chunks iteratively
        while True:
            try:
                df = next(df_iter)
                insert_batch(df=df, table_name=table_name, engine=engine)
            except StopIteration:
                print("Finished inserting all csv batches.")
                break


def preprocess(df, is_parquet):
    """
    Preprocess a df in-place
    :param df: df to preprocess in-place
    :param is_parquet: extra preprocessing for parquet files
    :return:
    """

    # Convert pickup/drop off datetime to "TIMESTAMP" data type
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    if is_parquet:
        preprocess_parquet(df)


def preprocess_parquet(df):
    """
    Preprocess a parquet file
    :param df:
    :return:
    """

    # Drop the extra "index" column
    df.drop(df.columns[0], axis=1, inplace=True)


def insert_header(df, table_name, engine, is_parquet=False):
    """
    Insert just the header into a DB
    :param df:
    :param table_name:
    :param engine:
    :param is_parquet:
    :return:
    """

    # Preprocess chunk
    preprocess(df, is_parquet=is_parquet)

    # insert just the row headers first
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    print('inserted row header inserted')


def insert_batch(df, table_name, engine, is_parquet=False):
    """
    Upload a batch of data from a dataframe to the database

    :param df: data frame
    :param table_name: table name
    :param engine: engine
    :param is_parquet: if the file is .parquet
    """
    t_start = time()

    # Preprocess chunk
    preprocess(df, is_parquet=is_parquet)

    # insert
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()

    print('inserted another chunk... %.3f seconds' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV or Parquet data to Postgres.')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', type=int, help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the data file')
    parser.add_argument('--parquet', type=bool, nargs='?', const=True,
                        default=True,
                        help='if the data file is parquet')

    args = parser.parse_args()

    main(args)



