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
            #Below is additional filter if required
            #select_query = select_query.limit(limit_value)
            #select_query = select_query.where(cls.my_table.c.last_name == '4YSEX8AY1Z' )
            result = connection.execute(select_query)

            # Fetch all rows into a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            # Display the first 5 records
            ##print(df.head())
            # Display the last 5 records
            #print(df.tail())
            # Display the info of the DataFrame i.e. the Null columns
            #print(df.info())
            # 15320 records, 0 nulls in every column
            # 1st Method. Display the DataFrame to show the nulls
            #print(df.isna())
            # 2nd Method. This gives results as a sum in a Column rather than table. 
            # GO WITH THIS TO REMOVE NAs
            #print(df.isna().sum())

            # *** NULL FIX ***
            # To replace NA values in DataFrame use inplace=True so don't need to assign back to df
            df.fillna(value=0, inplace=True)

            # Look through each column to understand the anomalies in the data
            # first_name_df = df['first_name']
            # print(first_name_df.describe()) # Summary
            # print(first_name_df.value_counts()) # Count of Unique Values

            # last_name_df = df['last_name']
            # print(last_name_df.describe()) # Summary
            # print(last_name_df.value_counts()) # Count of Unique Values

            # dob_df = df['date_of_birth']
            # print(dob_df.describe()) # Summary
            # print(dob_df.value_counts()) # Count of Unique Values

            # company_df = df['company']
            # print(company_df.describe()) # Summary
            # print(company_df.value_counts()) # Count of Unique Values

            # email_df = df['email_address']
            # print(email_df.describe()) # Summary
            # print(email_df.value_counts()) # Count of Unique Values

            # address_df = df['address']
            # print(address_df.describe()) # Summary
            # print(address_df.value_counts()) # Count of Unique Values

            # country_df = df['country']
            # print(country_df.describe()) # Summary
            # print(country_df.value_counts()) # Count of Unique Values

            # countryc_df = df['country_code']
            # print(countryc_df.describe()) # Summary
            # print(countryc_df.value_counts()) # Count of Unique Values

            # phone_df = df['phone_number']
            # print(phone_df.describe()) # Summary
            # print(phone_df.value_counts()) # Count of Unique Values

            # join_df = df['join_date']
            # print(join_df.describe()) # Summary
            # print(join_df.value_counts()) # Count of Unique Values

            # user_uuid_df = df['user_uuid']
            # print(user_uuid_df.describe()) # Summary
            # print(user_uuid_df.value_counts()) # Count of Unique Values

            # *** LAST NAME COLUMN FIX 
            # looked through the incorrect LAST NAME which showed whole row is wrong 
            # so will delete record (INDEX = 5309)
            # GO WITH THIS TO REMOVE LAST NAME ROW ERROR
            # row_to_print = df[df['last_name'] == '4YSEX8AY1Z']
            # # Set display to show all cols
            # pd.set_option('display.max_columns', None)
            # print(row_to_print)
            # pd.reset_option('display.max_columns')
            #index_to_delete = 5309
            #df.drop(index_to_delete, inplace=True)

            # *** ADDRESS ***
            # checking if \n (new line) is intentional or not
            # address_records = df['address'].sample(5)

            # for record in address_records:
            #     print(record)
            # is intentional so no need to remove

            # *** COUNTRY ***
            # UK = 9371, Germany = 4708, US = 1205, NULL = 21, GARBAGE = 15
            # need to remove anything that does not equal
            # the ~ negates the condition to 'not in' 
            row_to_print = df[~df['country'].isin(['United Kingdom', 'Germany', 'United States', 'NULL'])]
            print(row_to_print)
            #index_to_delete = [752, 1047, 2997, 3539, 5309, 6426, 8398, 9026, 10224, 10373, 11381, 12197, 13135, 14124, 14523]
            #df.drop(index_to_delete, inplace=True)
            # WILL NEED TO DELETE 15 rows as all COLUMNS HAVE ERRORS

            # *** COUNTRY CODE ***
            # GB = 9365, DE = 4708, US = 1205, NULL = 21, GGB = 6, GARBAGE = 15
            # First replace all GGB values correctly with GB
            #df['country'] = df['country'].str.replace('GGB', 'GB')
            ##df['country'].replace('GGB', 'GB', inplace=True)
            # WILL NEED TO DELETE 15 rows as all COLUMNS HAVE ERRORS (will be done in Country Fix)

            print(df.info())
        
# Call the query_data method to execute the query
