from process_transaction import Transaction
from dotenv import load_dotenv
import os


def main():
    # load env
    BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
    load_dotenv(os.path.join(BASE_DIR, '.env'))

    transactions = Transaction()

    # prepare transactions for db
    transactions.process_trxns(
        transactions.retrieve_trxns(os.getenv("TRXN_FILE_URL")), ['date', 'externalId', 'agentPhoneNumber',
                                                                  'transactionType', 'amount', 'balance',
                                                                  'receiverPhoneNumber', 'commission',
                                                                  'status', 'source'])


if __name__ == "__main__":
    main()
