import sqlalchemy
import pandas as pd

class DatabaseLoader:
    def __init__(self, config):
        user = config["user"]
        password = config["password"]
        host = config["host"]
        port = config["port"]
        db = config["database"]

        self.engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{db}"
        )

    def load_full(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        return len(df)

    def load_incremental(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists="append", index=False)
        return len(df)
