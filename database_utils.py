import yaml
from sqlalchemy import create_engine, engine, Table, MetaData, insert
import pandas as pd

class DatabaseConnector:
#Step 2
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
        return data
#Step 3    
    def __init__(self):
        self.engine = self.init_db_engine()
    
    def init_db_engine(self):
        # get credentials
        db_creds = self.read_db_creds()

        # get individual components
        RDS_HOST = db_creds['RDS_HOST'] 
        RDS_PASSWORD = db_creds['RDS_PASSWORD'] 
        RDS_USER = db_creds['RDS_USER']
        RDS_DATABASE = db_creds['RDS_DATABASE']
        RDS_PORT = db_creds['RDS_PORT']


        # Check for None values
        if None in (RDS_HOST, RDS_PASSWORD, RDS_USER, RDS_DATABASE, RDS_PORT):
            raise ValueError("One or more credentials is None. Check key names in the YAML file.")
        # Might be worth raising an exception to notify user 

        connection_string = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"

        # Print the generated connection string for Debugging
        #print("Generated Connection String:", connection_string)

        # Initialize and return the SQLAlchemy database engine
        engine = create_engine(connection_string)
        return engine
#Step 4
    def list_db_tables(self):
        # Get table name using MetaData
        # For Postgre is more specific to the PostgreSQL dialect. 
        # It explicitly uses the PostgreSQL dialect to get the table names.
        return self.engine.dialect.get_table_names(self.engine.connect())
    
#Step 7
    def upload_to_db(self, table_name, df):
        metadata = MetaData()
        metadata.reflect(bind=engine)
        target_table = metadata.tables[table_name]

        # Insert statement
        insert_stmt = insert(target_table).values(df.to_dict(orient='records'))

        with self.engine.connect() as connection:
            connection.execute(insert_stmt)

        #Step 8
    def upload_to_local_db(self, df):
        db_username = "postgres"
        db_password = "password"
        db_host = "localhost"  # Usually "localhost" if the database is on the same machine
        db_port = "5432"  # Usually 5432 for PostgreSQL
        db_name = "sales_db"

        # Construct the db_url
        db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

        try:
            local_engine = create_engine(db_url)

            # Try connection
            with local_engine.connect() as connection:
                print("connection successful!")

                # Create the table and upload data
                df.to_sql(name='dim_users', con=connection, index=False, if_exists='replace')
                print("Data uploaded successfully!")

        except Exception as e:
            print(f"Error connecting to the database: {e}")
  
# Call the methods
connector = DatabaseConnector()
db_creds = connector.read_db_creds()
engine = connector.init_db_engine()
tables = connector.list_db_tables()
print("Database Credentials:", db_creds)
print("Database Engine:", engine)
print("Tables in the database:", tables)


