import os
import sqlalchemy


class Connector:
    def __init__(self):
        self.pg_engine, self.pg_conn = self.get_pg_conn()

    def get_pg_conn(self):
        pg_engine = sqlalchemy.create_engine(
            "postgresql://{}:{}@{}/{}".format(
                os.getenv('PG_USER'),
                os.getenv('PG_PW'),
                os.getenv('PG_HOST'),
                os.getenv('PG_DBNAME')
            )
        )

        pg_conn = pg_engine.connect()

        return pg_engine, pg_conn
