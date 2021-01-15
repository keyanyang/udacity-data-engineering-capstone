import re
import sqlite3
from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create or get the current Spark session
    """
    # add sqlite jbdc jar for the use of SQLite DB with Spark
    # Spark 3.0.1 is used in my local so I've chosen the corresponding Spark packages and jars
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12")\
        .config("spark.jars", "./db_driver/sqlite-jdbc-3.32.3.2.jar") \
        .getOrCreate()
    return spark


def build_i94_port_dict(i94port_file_path):
    """
    Build I94 port dictionay stemming from i94port data file
    """
    i94port_name_code_dict = {}
    re_obj = re.compile(r'\'(.*)\'.*\'(.*)\'')

    with open(i94port_file_path) as f:
        for line in f:
            match = re_obj.search(line)
            i94port_name_code_dict[match[2].strip()] = match[1]
    return i94port_name_code_dict


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file, timeout=10)
    except Error as e:
        print(e)

    return conn


def save_table_to_database(df, table_name, db_url):
    """
    Load the data from Spark dataframe to database
    """
    # If other database like Redshift is used, we can config the parameter numPartitions as SQLite
    # only allows one connection at a time.
    df.write\
    .format('jdbc')\
    .mode("overwrite")\
    .options(url=f'jdbc:sqlite:{db_url}',\
     dbtable=table_name, driver='org.sqlite.JDBC', numPartitions=1).save()


def check_db_table_size(table_name, db_url):
    """
    Validate the number of records in each table in database
    """
    conn = create_connection(db_url)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    cnt = cur.fetchall()[0][0]
    if cnt == 0:
        raise ValueError(f'Table {table_name} is empty.')
    print(f"Count check passed for {table_name}")
    cur.close()
