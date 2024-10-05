import sqlite3
import pandas as pd

class DatabaseExtractor:
    def __init__(self, db_path, db_type='sqlite'):
        self.db_path = db_path
        self.db_type = db_type

    def extract_table(self, table_name):
        if self.db_type != 'sqlite':
            raise ValueError("Only sqlite is supported in this demo project")

        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
