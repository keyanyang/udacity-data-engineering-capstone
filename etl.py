import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv

from src.utility import *
from src.data_model import *

# load environment variables
load_dotenv(find_dotenv())
DATABASE_URL = os.getenv("DB_URL")

def main():
    """
    - Load data from different sources
    
    - Process Spark Dataframes
    
    - Build the database and tables
    """
    spark = create_spark_session()

    # read data
    df_i94 = spark.read.parquet("./data/raw/sas_data")
    df_airport = spark.read.csv("./data/raw/airport-codes_csv.csv", header=True, inferSchema=True)
    df_demo = spark.read.csv("./data/raw/us-cities-demographics.csv", header=True, inferSchema=True, sep=';')
    df_temp = spark.read.csv("./data/raw/GlobalLandTemperaturesByCity.csv", header=True, inferSchema=True)

    # drop duplicates
    df_i94 = df_i94.drop_duplicates(['cicid'])
    df_airport = df_airport.drop_duplicates(['ident'])
    df_demo = df_demo.drop_duplicates(['City', 'State', 'Race'])
    df_temp = df_temp.drop_duplicates(['dt', 'City', 'Country'])

    # drop missing
    df_i94 = df_i94.dropna(how='all')
    df_airport = df_airport.dropna(how='all')
    df_demo = df_demo.dropna(how='all')
    df_temp = df_temp.dropna(how='all')

    # drop others
    df_i94 = df_i94.drop('occup', 'entdepu','insnum')
    df_temp = df_temp.dropna(subset=['AverageTemperature'])

    i94port_name_code_dict = build_i94_port_dict('./data/raw/i94port.txt')
    i94port_codes = [code for name, code in i94port_name_code_dict.items()]

    # clean i94 df
    df_i94 = df_i94.filter(df_i94.i94port.isin(i94port_codes))

    # create tables
    i94_fact = create_i94_fact(df_i94)
    visa_dim = create_visa_dim(df_i94)
    temperature_dim = create_temperature_dim(df_temp, i94port_name_code_dict)
    airport_dim = create_airport_dim(df_airport, i94port_name_code_dict)
    demo_dim = create_demographics_dim(df_demo, i94port_name_code_dict)

    output_tables = {
        "i94_fact": i94_fact,
        "visa_dim": visa_dim,
        "temperature_dim": temperature_dim,
        "airport_dim": airport_dim,
        "demo_dim": demo_dim
    }

    # save data into database
    for name, table in output_tables.items():
        save_table_to_database(table, name, DATABASE_URL)

    print("ETL is completed.")

if __name__ == "__main__":
    main()
