from custom_modules.helper import insert_user, retrieve_from_db
from custom_modules.retrieve_data import retrieve_file
from custom_modules.process_transaction import process_trxns
import pandas as pd
from custom_modules.schema import User
from sqlalchemy import select, func
from custom_modules.logs import log
import logging
from datetime import datetime


def main(db_user: str, db_host: str, db_pw: str, db_name: str,
         bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str,
         slack_url: str):

    # check last time db was updated
    max_updated = retrieve_from_db(
        select(func.max(User.updatedAt)), db_user, db_host, db_pw, db_name)

    if max_updated[0]["max"].date() == datetime.today().date():
        log("Today's transactions already processed",
            logging.INFO, logging.info)
    else:
        # retrieve transactions from s3
        df = retrieve_file(bucket_name, aws_access_key_id,
                           aws_secret_access_key, slack_url)

        if type(df) == pd.DataFrame:

            # prepare transactions for db
            df = process_trxns(df, ['date', 'externalId', 'agentPhoneNumber',
                                    'transactionType', 'amount', 'balance',
                                    'receiverPhoneNumber', 'commission',
                                    'status', 'source'])
            # push data to db
            df.apply(lambda row: insert_user(
                row, db_user, db_host, db_pw, db_name), axis=1)
