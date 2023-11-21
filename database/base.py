import os
import sqlalchemy
from logs import log
import logging


class Connector:
    def __init__(self):
        log("Initializing database engine and connection",
            logging.INFO, logging.info)
        self.pg_engine, self.pg_conn = self.get_pg_conn()

    def get_pg_conn(self):
        try:
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
        except Exception as e:
            log(
                f"An error occured creating database connection error:{e}", logging.ERROR, logging.error)
