class DataCleaner:
    def clean(self, df, table_name):
        # Very basic cleaning for demo
        df = df.copy()
        df = df.dropna(how='all')
        return df
