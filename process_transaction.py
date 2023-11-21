import pandas as pd
import time
from logs import log
import logging
from datetime import datetime


class Transaction():
    """ Retrieve and process transaction data"""

    def unix_timestamp_in_ms(self, datetime_str):
        try:
            datetime_object = datetime.strptime(
                datetime_str, '%Y-%m-%d %H:%M:%S')
            unix_timestamp_ms = time.mktime(datetime_object.timetuple()) * 1000

            return int(unix_timestamp_ms)
        except Exception as e:
            log(
                f"An error occured while converting date;{datetime_object}, to unix timestamp in ms, error:{e}", logging.ERROR, logging.error)

    def retrieve_trxns(self, file_path: str) -> pd.DataFrame:
        try:
            log("Retrieving transactions from csv file",
                logging.INFO, logging.info)

            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            log(f"An error occured while retrieving transaction data, error:{e}",
                logging.ERROR, logging.error)

    def process_trxns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        try:
            log("Processing retrieved transactions",
                logging.INFO, logging.info)

            # checking missing column issues
            for column in columns:
                if column not in df.columns:
                    df[column] = None

            # convert date to unix timestamp
            log("Converting date to unix timestamp in ms",
                logging.INFO, logging.info)

            # df.date = pd.to_datetime(df.date, errors="ignore")
            df.date = df.date.apply(lambda row: self.unix_timestamp_in_ms(row))

            # convert timestamp of agent phone number
            log("Type casting columns",
                logging.INFO, logging.info)
            df.agentPhoneNumber = df.agentPhoneNumber.astype(str)
            df.balance = df.balance.astype(float)
            df.amount = df.amount.astype(float)
            df.commission = df.commission.astype(float)

            return df

        except Exception as e:
            log(f"An error occured while processing transaction data, error:{e}",
                logging.ERROR, logging.error)
