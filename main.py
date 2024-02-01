from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#Instantiate the classes
connector = DatabaseConnector()
extractor = DataExtractor()
cleaning = DataCleaning()

# Read database credentials
db_creds = connector.read_db_creds()
print("Database Credentials:", db_creds)

# Initialize the database engine
engine = connector.init_db_engine()
# TASK 3 STEP 4
# Set the engine attribute in your connector instance
connector.engine = engine
print("Database Engine:", engine)

# TASK 3 STEP 4
# List all tables in the database
tables = connector.list_db_tables()
print("Database Tables:", tables)

# Test 'users table' from list_db_tables
table_name = 'legacy_users'

# Read the table into a Pandas DataFrame
df = extractor.read_rds_table(connector, table_name)
print(df.head())  # Display the first few rows of the DataFrame

# TASK 3 STEP 6
# Check shape before cleaning
print("Before cleaning:", df.shape)
# Pass df to DataCleaning methods
cleaned_df = cleaning.clean_user_data(df)
# Check shape after cleaning 
print("After cleaning:", cleaned_df.shape)
