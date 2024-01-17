from database_utils import DatabaseConnector
from data_extraction import DataExtractor

# Instantiate the classes
connector = DatabaseConnector()
extractor = DataExtractor(connector)

# Use init_db_engine method to create an engine
engine = connector.init_db_engine()
db_creds = connector.read_db_creds()
tables = connector.list_db_tables()

# Use the read_rds_table method through the DataExtractor instance
df = extractor.read_rds_table('legacy_users')

print("Database Credentials:", db_creds)
print("Database Engine:", engine)
print("Tables in the database:", tables)
print(df.head())
