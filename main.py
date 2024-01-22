from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Instantiate the classes
connector = DatabaseConnector()
extractor = DataExtractor(connector)
cleaner = DataCleaning(connector)

# Use init_db_engine method to create an engine
db_creds = connector.read_db_creds()
engine = connector.init_db_engine()
tables = connector.list_db_tables()

# Use the read_rds_table method through the DataExtractor instance
# print("Before read_rds_table method call") # Task 3 Step 4 # Not Working
#df = extractor.read_rds_table('legacy_users')
# print("After read_rds_table method call")
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_df = extractor.retrieve_pdf_data(link)
clean_card = cleaner.clean_card_data(pdf_df)

print("Database Credentials:", db_creds)
print("Database Engine:", engine)
print("Tables in the database:", tables)
#print(df.head())
print(clean_card)
#print(clean_card)