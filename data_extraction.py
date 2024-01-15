from database_utils import DatabaseConnector
from sqlalchemy import create_engine, MetaData, Table, select
import yaml

class DataExtractor:
#Step 6
    def clean_user_data(self, table_name):
        # Reflect the table using the metadata
        table = Table(table_name, self.metadata, autoload=True)

        # Create a simple select statement
        select_statement = SELECT * FROM [table]

        # Execute the select statement
        result = self.engine.execute(select_statement)

        # Fetch the results into a list of dictionaries
        rows = result.fetchall()

        # Optional: Convert the result to a DataFrame for easier handling
        df = pd.DataFrame(rows, columns=result.keys())

        return df
    
connector = DatabaseConnector()
tables = connector.list_db_tables()
print("Tables:", tables)

# Assuming 'TableA' is in the list of tables
users_data = connector.query_table('legacy_users')
print("Users Data:")
print(users_data)


