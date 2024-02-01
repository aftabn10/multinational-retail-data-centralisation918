from database_utils import DatabaseConnector
from sqlalchemy import text
import pandas as pd

class DataExtractor:
# TASK 3 STEP5
    def __init__(self):
        pass

    def read_rds_table(self, db_connector, table_name):
        # Get the engine from the DatabaseConnector
        engine = db_connector.engine

        # Use the engine to read the table into a Pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(sql=text(query), con=engine.connect())

        return df