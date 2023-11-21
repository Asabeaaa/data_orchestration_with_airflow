import sqlalchemy
from custom_modules.logs import log
import logging


class Connector:
    def __init__(self, db_user: str, db_host: str, db_pw: str, db_name: str):

        log("Initializing database engine and connection",
            logging.INFO, logging.info)

        self.db_user = db_user
        self.db_host = db_host
        self.db_pw = db_pw
        self.db_name = db_name

    def get_pg_engine(self):
        try:
            pg_engine = sqlalchemy.create_engine(
                f"postgresql://{self.db_user}:{self.db_pw}@{self.db_host}/{self.db_name}"
            )

            return pg_engine
        except Exception as e:
            log(
                f"An error occured creating database connection error:{e}", logging.ERROR, logging.error)
