# Step 6 
# Create a method called clean_user_data in the DataCleaning class which 
# will perform the cleaning of the user data.

# You will need clean the user data, look out for NULL values, 
# errors with dates, incorrectly typed values and rows filled with the 
# wrong information.

from database_utils import DatabaseConnector
from sqlalchemy import Table, MetaData, select, engine, create_engine
import pandas as pd

class DataCleaning:
    metadata = MetaData()
    metadata.reflect(bind=engine)  # Reflect the existing database tables

    my_table = metadata.tables['legacy_users']  # Access the 'users' table directly

    @classmethod
    def query_data(cls):
        #limit_value = 10
        with engine.connect() as connection:
            select_query = select(cls.my_table.c)  # Select all columns from the table
            result = connection.execute(select_query)

            # Fetch all rows into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            # *** NULL FIX ***
            # To replace NA values in DataFrame use inplace=True so don't need to assign back to df
            df.fillna(value=0, inplace=True)

            # *** LAST NAME COLUMN FIX 
            # looked through the incorrect LAST NAME which showed whole row is wrong 
            # so will delete record (INDEX = 5309)
            # GO WITH THIS TO REMOVE LAST NAME ROW ERROR
            row_to_print = df[df['last_name'] == '4YSEX8AY1Z']
            # # Set display to show all cols
            pd.set_option('display.max_columns', None)
            # print(row_to_print)
            pd.reset_option('display.max_columns')
            index_to_delete = 5309
            df.drop(index_to_delete, inplace=True)

            # *** COUNTRY ***
            # UK = 9371, Germany = 4708, US = 1205, NULL = 21, GARBAGE = 15
            # need to remove anything that does not equal
            # the ~ negates the condition to 'not in' 
            row_to_print = df[~df['country'].isin(['United Kingdom', 'Germany', 'United States', 'NULL'])]
            #print(row_to_print)
            index_to_delete = [752, 1047, 2997, 3539, 5309, 6426, 8398, 9026, 10224, 10373, 11381, 12197, 13135, 14124, 14523]
            df.drop(index_to_delete, inplace=True)
            # WILL NEED TO DELETE 15 rows as all COLUMNS HAVE ERRORS

            # *** COUNTRY CODE ***
            # GB = 9365, DE = 4708, US = 1205, NULL = 21, GGB = 6, GARBAGE = 15
            # First replace all GGB values correctly with GB
            df['country'] = df['country'].str.replace('GGB', 'GB')
            #df['country'].replace('GGB', 'GB', inplace=True)
            # WILL NEED TO DELETE 15 rows as all COLUMNS HAVE ERRORS (will be done in Country Fix)

            print(df.info())
        
# Call the query_data method to execute the query
