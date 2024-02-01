class DataCleaning:
#TASK STEP 6     
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df_filled = df.fillna(value=0)

        return df_filled
