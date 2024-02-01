import yaml
from sqlalchemy import create_engine, engine, Table, MetaData, insert, inspect

class DatabaseConnector:
    def __init__(self):
        self.engine = None
# TASK 3 STEP 2     
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
        return data
# TASK 3 STEP 3    
    def init_db_engine(self):
        db_creds = self.read_db_creds()
    
    # Construct the connection string
        connection_string = (
            f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@"
            f"{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        )

        # Create and return the SQLAlchemy engine
        self.engine = create_engine(connection_string)
        
        with self.engine.connect():
            pass

        return self.engine

# TASK 3 STEP 3    
    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()





