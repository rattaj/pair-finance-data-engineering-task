import json
import string
import logging
import sys

import sqlalchemy as db
import pandas as pd
from os import environ
from time import sleep

from geopy.distance import distance
from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

# print('Waiting for the data generator...')
# sleep(20)
# print('ETL Starting...')
#
# while True:
#     try:
#         psql_engine = db.create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
#         break
#     except OperationalError:
#         sleep(0.1)
# print('Connection to PostgresSQL successful.')

# Write the solution here
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.set_option('display.max_colwidth', None)


def get_db_engine_connection(connection_string):
    while True:
        try:
            return db.create_engine(connection_string, pool_pre_ping=True, pool_size=10)
        except OperationalError:
            sleep(0.1)


def read_data_from_db_table(db_engine: Engine, table_name: string) -> DataFrame:
    return pd.read_sql(f'select * from {table_name}', con=db_engine)


def group_data_by_device_id_and_hour(df: DataFrame) -> DataFrameGroupBy:
    df.set_index('time', inplace=True)
    return df.groupby(['device_id', pd.Grouper(freq='H')])


def get_max_temperature_per_group(df_grouped: DataFrameGroupBy) -> DataFrame:
    return df_grouped['temperature'].max()


def get_hourly_data_points_per_device(df_grouped: DataFrameGroupBy) -> DataFrame:
    return df_grouped.size()


def calculate_distance(row: DataFrame) -> DataFrame:
    coordinates = list(zip(row['latitude'], row['longitude']))
    return round(sum(distance(coordinates[i], coordinates[i + 1]).km for i in range(len(coordinates) - 1)), 3)


def format_column_types(df: DataFrame) -> DataFrame:
    df['time'] = pd.to_datetime(df['time'].astype(int), unit='s')
    df['latitude'] = df['location'].apply(lambda x: json.loads(x)['latitude']).astype(float)
    df['longitude'] = df['location'].apply(lambda x: json.loads(x)['longitude']).astype(float)
    return df


def store_devices_statistics(engine, table_name: string, df: DataFrame):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logger.info(f'Successfully stored the data into the table {table_name}.')


def close_db_engine(db_engine):
    try:
        if db_engine is not None:
            db_engine.dispose()
    except Exception as e:
        logger.info('Failed to close the database connection ', db_engine, e)


def process_data(df: DataFrame) -> DataFrame:
    devices_data = format_column_types(df)

    devices_data_grouped = group_data_by_device_id_and_hour(devices_data)

    max_temperatures = get_max_temperature_per_group(devices_data_grouped)

    data_point_counts = get_hourly_data_points_per_device(devices_data_grouped)

    total_distances = devices_data.groupby(['device_id', pd.Grouper(freq='H')]).apply(calculate_distance)

    result_df = pd.DataFrame({'Max Temperature': max_temperatures,
                              'Data Point Count': data_point_counts,
                              'Total Distance (km)': total_distances})

    result_df.reset_index(inplace=True)

    return result_df


def devices_data_etl():
    psql_engine = get_db_engine_connection(environ["POSTGRESQL_CS"])
    mysql_engine = None
    try:
        logger.info('Read devices data from the postgres database.')
        devices_df = read_data_from_db_table(psql_engine, 'devices')
        logger.info(f'Devices data: \n{devices_df.head()}')

        logger.info('Apply aggregations on the devices data.')
        result_df = process_data(devices_df)
        logger.info(f'Aggregated data: \n{result_df.head()}')

        logger.info('Store the processed data into the MySQL database.')
        mysql_engine = get_db_engine_connection(environ["MYSQL_CS"])
        store_devices_statistics(mysql_engine, 'devices_statistics', result_df)

        logger.info('Retrieve the results from the MySql table devices_statistics for verification.')
        stored_data = read_data_from_db_table(mysql_engine, 'devices_statistics')
        logger.info(f'Stored results: \n{stored_data.head()}')

    except Exception as e:
        logger.info('ETL process failed.')
        raise e
    finally:
        close_db_engine(psql_engine)
        close_db_engine(mysql_engine)


if __name__ == '__main__':
    logger.info('Waiting for the data generator...')
    sleep(20)
    logger.info('ETL Starting...')
    devices_data_etl()
