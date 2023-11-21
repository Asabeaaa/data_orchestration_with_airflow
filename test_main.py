from process_transaction import Transaction
import pandas as pd
import unittest
from database.helper import UpdateDB
import os
from dotenv import load_dotenv


class TestFlow(unittest.TestCase):

    # load env
    BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
    load_dotenv(os.path.join(BASE_DIR, '.env'))

    def test_unix_conversion(self):
        # checking return value and data type
        transactions = Transaction()

        # value, result = "2023-06-01 11:57:41", 1685620661000
        value, result = "2023-06-01 11:57:41", "1685620661000"
        assert transactions.unix_timestamp_in_ms(value) == result

    def test_unix_conversion_exceptions(self):
        transactions = Transaction()

        with self.assertRaises(Exception) as context:
            transactions.unix_timestamp_in_ms(3)

        print(context.exception)

    def test_data_insert(self):

        data = [{"agentPhoneNumber": "1234567",
                "transactionType": "deposit",
                 "amount": 344.0,
                 "commission": 3445.0,
                 "balance": 4646.0,
                 "externalId": "c3674bd214f4221bee3eb63592f94637c483445d539d9a45207d961044a9e5de",
                 "date": "1685620661000",
                 "source": "In flows",
                 "status": "Pending",
                 "receiverPhoneNumber": "26262626"}]

        df = pd.DataFrame(data)

        # push data to db
        db_process = UpdateDB()

        value, result = df, "Transaction processed"
        x = value.apply(lambda row: db_process.insert_user(row), axis=1)
        assert x == result


if __name__ == '__main__':
    test = TestFlow()

    # test.test_unix_conversion()

    # test.test_unix_conversion_exceptions()

    test.test_data_insert()
