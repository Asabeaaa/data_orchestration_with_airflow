from sqlalchemy.orm import sessionmaker
from database.schema import Transaction, User
from database.base import Connector
from typing import Dict, List
from sqlalchemy import select
import pandas as pd
import uuid
from logs import log
import logging
from datetime import datetime


class UpdateDB(Connector):
    def retrieve_from_db(self, stmt: str) -> List[Dict[str, any]]:
        try:
            log(
                f"Retrieving data from db", logging.INFO, logging.info)

            factory = sessionmaker(bind=self.pg_engine)
            session = factory()
            results = session.execute(stmt)
            results = [row._asdict() for row in results.all()]
            return results

        except Exception as e:
            log(
                f"An error occured while retrieving data from the database, error:{e}", logging.ERROR, logging.error)

        finally:
            session.close()

    def update_user_transaction(self, number_of_transactions: int, user_uuid: uuid.uuid4):
        try:
            log(
                f"Updating number of transactions for user: {user_uuid} to {number_of_transactions}", logging.INFO, logging.info)
            factory = sessionmaker(bind=self.pg_engine)
            session = factory()
            session.query(User).filter(User.uuid == user_uuid).update(
                {"nTransactions": number_of_transactions, "updatedAt": datetime.now()})
            session.commit()

        except Exception as e:
            log(
                f"An error occured while updating user number of transactions, error:{e}", logging.ERROR, logging.error)

        finally:
            session.close()

    def insert_user(self, row: pd.Series):
        try:
            log(
                "Processing data for db", logging.INFO, logging.info)

            factory = sessionmaker(bind=self.pg_engine)
            session = factory()

            # insert new user
            new_user_uuid = uuid.uuid4()
            agent_number = row["agentPhoneNumber"]

            log(
                f"Inserting in db user: {new_user_uuid}, phone number: {agent_number}", logging.INFO, logging.info)

            new_user = User(
                phoneNumber=agent_number, nTransactions=0, uuid=new_user_uuid)

            session.add(new_user)
            session.commit()

            # insert transaction
            response = self.insert_transaction(row, new_user_uuid)

            if response:
                # update user transaction to 1
                self.update_user_transaction(1, new_user_uuid)

        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e):

                log(f"User with phone number: {agent_number} already exists",
                    logging.INFO, logging.info)

                # retrieve user with that phone number
                user = self.retrieve_from_db(stmt=select(User.uuid, User.nTransactions).where(
                    User.phoneNumber == agent_number))

                old_user_uuid = user[0]["uuid"]
                number_of_user_trnxs = user[0]["nTransactions"]

                # insert transaction
                response = self.insert_transaction(row, old_user_uuid)

                if response:
                    # update user transaction + 1
                    self.update_user_transaction(
                        number_of_user_trnxs + 1, old_user_uuid)

            else:
                log(
                    f"An error occured while processing user and transaction data for db, error:{e}", logging.ERROR, logging.error)

        finally:
            session.close()

    def insert_transaction(self, row: pd.Series, user_uuid: uuid.uuid4):
        try:
            new_transaction_uuid = uuid.uuid4()

            log(f"Inserting transaction:{new_transaction_uuid} for user: {user_uuid}",
                logging.INFO, logging.info)

            factory = sessionmaker(bind=self.pg_engine)
            session = factory()

            # insert transaction
            new_transaction = Transaction(mobile=row["receiverPhoneNumber"], commission=row["commission"],
                                          balance=row["balance"], amount=row["amount"], externalId=row["externalId"],
                                          requestTimestamp=row["date"], updateTimestamp=row["date"],
                                          userUuid=user_uuid, uuid=new_transaction_uuid, category=row[
                                              "transactionType"],
                                          status=row["status"], source=row["source"])

            session.add(new_transaction)
            session.commit()
            return "Transaction inserted"

        except Exception as e:
            log(
                f"An error occured while inserting transaction data to db, error:{e}", logging.ERROR, logging.error)

        finally:
            session.close()
