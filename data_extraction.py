from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor:
    def __init__(self, connector):
        self.connector = connector

    def read_rds_table(self, table_name):
        try:
            # Use the init_db_engine method to create an engine
            engine = self.connector.init_db_engine()

            # Use the engine to extract the table to a pandas DataFrame
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, engine)

            return df
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        
    # Task 4 Step 2    
    # retrieve_pdf_data
    # Works
    def retrieve_pdf_data(self, link):
        pdf_df = tabula.read_pdf(link, pages='all')
        return pdf_df
    