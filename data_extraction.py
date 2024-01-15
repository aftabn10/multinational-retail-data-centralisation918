from database_utils import DatabaseConnector
from sqlalchemy import create_engine, MetaData, Table, select
import yaml

class DataExtractor:
    #Step 5
    def read_rds_table(self, table_name):

        query = f"SELECT * FROM {table_name}"
        result = self.engine.execute(query)

        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

