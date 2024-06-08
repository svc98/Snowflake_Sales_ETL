import os
import sys
import logging
from pathlib import Path
import snowflake.connector
from snowflake.snowpark import Session

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%I:%M:%S')


def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": "gasylia-eob07677",
        "USER": "snowpark_user",
        "PASSWORD": "Password123",
        "ROLE": "SYSADMIN",
        "DATABASE": "SALES_DWH",
        "SCHEMA": "BRONZE",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
    }
    return Session.builder.configs(connection_parameters).create()

def get_snowflake_connector():
    connection_parameters = {
        "account": "gasylia-eob07677",
        "user": "snowpark_user",
        "password": "Password123",
        "role": "SYSADMIN",
        "database": "SALES_DWH",
        "schema": "BRONZE",
        "warehouse": "SNOWPARK_ETL_WH"
    }
    return snowflake.connector.connect(**connection_parameters)

def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []       # List to store CSV file paths
    partition_dir = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)

    return file_name, partition_dir, local_file_path

def get_files_in_stage(conn, stage_location):
    query = f"LIST {stage_location}"
    cur = conn.cursor()
    cur.execute(query)
    stage_files = [row[0] for row in cur.fetchall()]
    cur.close()
    return stage_files


def main():
    # Specify the directory path to traverse
    root = str(Path(__file__).parent.parent)
    sales_data_path = root + '/data/sales'
    exchange_rate_path = root + '/data/exchange_rate/exchange_rate.csv'

    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(sales_data_path, '.csv')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(sales_data_path, '.json')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(sales_data_path, '.parquet')
    session = get_snowpark_session()
    conn = get_snowflake_connector()
    stage_location = '@SALES_DWH.BRONZE.INTERNAL_LOADING_STAGE'

    # Get the list of files already in the Snowflake stage
    stage_files = get_files_in_stage(conn, stage_location)

    # CSV,JSON & Parquet Sales data handling:
    for index, file_path in enumerate(csv_local_file_path):
        stage_file_folder = stage_location + csv_partition_dir[index]
        stage_file_path = stage_file_folder + "/" + csv_file_name[index]
        if stage_file_path not in stage_files:
            put_result = session.file.put(file_path, stage_file_folder, auto_compress=False, overwrite=True, parallel=10)
            print(put_result)
        else:
            print(f"File {stage_file_path} already exists. Skipping upload.")

    for index, file_path in enumerate(json_local_file_path):
        stage_file_folder = stage_location + json_partition_dir[index]
        stage_file_path = stage_file_folder + "/" + json_file_name[index]
        if stage_file_path not in stage_files:
            put_result = session.file.put(file_path, stage_file_folder, auto_compress=False, overwrite=True, parallel=10)
            print(put_result)
        else:
            print(f"File {stage_file_path} already exists. Skipping upload.")

    for index, file_path in enumerate(parquet_local_file_path):
        stage_file_folder = stage_location + parquet_partition_dir[index]
        stage_file_path = stage_file_folder + "/" + parquet_file_name[index]
        if stage_file_path not in stage_files:
            put_result = session.file.put(file_path, stage_file_folder, auto_compress=False, overwrite=True, parallel=10)
            print(put_result)
        else:
            print(f"File {stage_file_path} already exists. Skipping upload.")

    # Exchange Rate data handling:
    stage_file_path = stage_location + "/" + os.path.basename(exchange_rate_path)
    if stage_file_path not in stage_files:
        put_result = session.file.put(exchange_rate_path, stage_file_path, auto_compress=False, overwrite=True, parallel=10)
        print(put_result)
    else:
        print(f"File {stage_file_path} already exists. Skipping upload.")


if __name__ == '__main__':
    main()
