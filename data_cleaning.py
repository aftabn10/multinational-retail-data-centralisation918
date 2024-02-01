class DataCleaning:
#TASK STEP 6     
    def __init__(self):
        pass

    def clean_user_data(self, df):
        valid_countries = ['United Kingdom', 'Germany', 'United States', 'NULL']
        
        # Create a copy of df
        df_filled = df.copy()
        # Fill NaN values with 0
        df_filled = df.fillna(value=0)
        # Drop columns with all NaN values
        df_filled = df.dropna(axis=1, how='all')
        # Remove row with specific last name
        df_filled = df[df['last_name'] != '4YSEX8AY1Z']
        # Filter rows based on valid countries
        df_filled = df[df['country'].isin(valid_countries)]
        # Replace values in the country column
        df_filled['country'] = df['country'].str.replace('GGB', 'GB')

        return df_filled
