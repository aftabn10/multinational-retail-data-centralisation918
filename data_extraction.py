from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor:
    def __init__(self, connector):
        self.connector = connector

    def read_rds_table(self, table_name):
        # Use the list_db_tables method to get the list of tables
        engine = self.connector.init_db_engine()

        # Use the engine to extract the table to a pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)

        return df

# # Call the methods
# connector = DatabaseConnector()
# data_extractor = DataExtractor(connector)
# print("Tables in the database:", data_extractor.connector.list_db_tables())
# # Test reading a non-user data table
# df = data_extractor.read_rds_table('legacy_users')
# print("Legacy Users:")
# print(df.head())