import pandas as pd
import time


class Transaction():
    def unix_timestamp_in_ms(self, timestamp):
        unix_timestamp_ms = time.mktime(timestamp.timetuple()) * 1000
        return unix_timestamp_ms

    def retrieve_trxns(self, file_path: str) -> pd.DataFrame:
        df = pd.read_csv(file_path)
        return df

    def process_trxns(self, df: pd.DataFrame) -> pd.DataFrame:
        # convert date to unix timestamp
        df.date = pd.to_datetime(df.date, errors="ignore")
        df.date = df.date.apply(lambda row: self.unix_timestamp_in_ms(row))

        # convert timestamp of agent phone number
        df.agentPhoneNumber = df.agentPhoneNumber.astype(str)
        return df
