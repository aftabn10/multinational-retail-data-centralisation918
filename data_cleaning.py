# Step 6 
# Create a method called clean_user_data in the DataCleaning class which 
# will perform the cleaning of the user data.

# You will need clean the user data, look out for NULL values, 
# errors with dates, incorrectly typed values and rows filled with the 
# wrong information.

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from sqlalchemy import Table, MetaData, select, engine, create_engine
import pandas as pd

class DataCleaning:
    def __init__(self, connector):
        self.connector = connector
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.connector.engine)
        self.my_table = self.metadata.tables['legacy_users']

    def query_data(self):
        with self.connector.engine.connect() as connection:
            select_query = select(self.my_table.c)
            result = connection.execute(select_query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df

    def clean_user_data(self):
        df = self.query_data()

        # *** NULL FIX ***
        df.fillna(value=0, inplace=True)

        # *** LAST NAME COLUMN FIX ***
        # Remove rows where 'last_name' is '4YSEX8AY1Z'
        df = df[df['last_name'] != '4YSEX8AY1Z']

        # *** COUNTRY ***
        # Remove rows where 'country' is not in the specified list
        valid_countries = ['United Kingdom', 'Germany', 'United States', 'NULL']
        df = df[df['country'].isin(valid_countries)]

        # *** COUNTRY CODE ***
        # Replace 'GGB' with 'GB'
        df['country'] = df['country'].str.replace('GGB', 'GB')

        # Print information about the cleaned DataFrame
        print(df.info())

        # Return the cleaned DataFrame
        return df

    def clean_card_data(self, pdf_df):
         # *** Analysis ***
         print("Data Analysis:")
         print(pdf_df.describe())
         # Add more analysis steps based on your requirements
        
         return pdf_df