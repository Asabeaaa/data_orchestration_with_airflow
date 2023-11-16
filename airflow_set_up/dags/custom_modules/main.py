from custom_modules.helper import insert_user
from custom_modules.retrieve_data import retrieve_file
from custom_modules.process_transaction import process_trxns
import pandas as pd

# def main(db_user: str, db_host: str, db_pw: str, db_name: str, bucket_name: str):

#     data = [{"agentPhoneNumber": "1234567",
#              "transactionType": "deposit",
#              "amount": "344",
#              "commission": "3445",
#              "balance": "4646",
#              "externalId": "c3674bd214f4221bee3eb63592f94637c483445d539d9a45207d961044a9e5de",
#              "date": "1685629904000",
#              "source": "In flows",
#              "status": "Pending",
#              "receiverPhoneNumber": "26262626"}]

#     df = pd.DataFrame(data)
#     # push data to db

#     df.apply(lambda row: insert_user(
#         row, db_user, db_host, db_pw, db_name), axis=1)


def main(db_user: str, db_host: str, db_pw: str, db_name: str,
         bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str,
         slack_url: str):

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
