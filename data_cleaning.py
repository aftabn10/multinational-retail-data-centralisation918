import numpy as np

class DataCleaning:
#TASK STEP 6     
    def __init__(self):
        pass

    def clean_user_data(self, df):
        valid_countries = ['United Kingdom', 'Germany', 'United States', 'NULL']
        
        # Create a copy of df
        df_filled = df.copy()
        # Fill NaN values with 0
        df_filled = df_filled.fillna(value=0)
        # Drop columns with all NaN values
        df_filled = df_filled.dropna(axis=1, how='all')
        # Remove row with specific last name
        df_filled = df_filled[df_filled['last_name'] != '4YSEX8AY1Z']
        # Filter rows based on valid countries
        df_filled = df_filled[df_filled['country'].isin(valid_countries)]
        # Replace values in the country column
        df_filled['country'] = df_filled['country'].str.replace('GGB', 'GB')

        return df_filled
