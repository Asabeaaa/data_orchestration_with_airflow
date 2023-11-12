from process_transaction import Transaction
from dotenv import load_dotenv
import os
import pandas as pd
from database.helper import UpdateDB


def main():
    # load env
    BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
    load_dotenv(os.path.join(BASE_DIR, '.env'))

    transactions = Transaction()

    # prepare transactions for db
    df = transactions.process_trxns(
        transactions.retrieve_trxns(os.getenv("TRXN_FILE_URL")))

    # push data to db
    db_process = UpdateDB()
    df[0:1].apply(lambda row: db_process.update_pg_db(row), axis=1)


if __name__ == "__main__":
    main()
