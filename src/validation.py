import os
from dotenv import load_dotenv, find_dotenv
from src.utility import check_db_table_size

# load environment variables
load_dotenv(find_dotenv())
DATABASE_URL = os.getenv("DB_URL")
print(f"Checking {DATABASE_URL}")

tables = ["i94_fact",
          "visa_dim",
          "temperature_dim",
          "airport_dim",
          "visa_dim"
         ]

for table in tables:
    check_db_table_size(table, DATABASE_URL)
