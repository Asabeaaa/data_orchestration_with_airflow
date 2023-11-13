from sqlalchemy.orm import sessionmaker
from database.schema import Transaction, User
from database.base import Connector
from typing import Dict, List
from sqlalchemy import select
import pandas as pd
import uuid


class UpdateDB(Connector):
    def retrieve_from_db(self, stmt: str) -> List[Dict[str, any]]:
        try:
            factory = sessionmaker(bind=self.pg_engine)
            session = factory()
            results = session.execute(stmt)
            results = [row._asdict() for row in results.all()]
            return results

        except Exception as e:
            print(f"Error: {e}")

        finally:
            session.close()

    def insert_user(self, row: pd.Series):
        # print(row)
        try:
            factory = sessionmaker(bind=self.pg_engine)
            session = factory()

            # insert new user
            new_user_uuid = uuid.uuid4()
            new_user = User(
                phoneNumber=row["agentPhoneNumber"], nTransactions=0, uuid=new_user_uuid)

            session.add(new_user)
            session.commit()

            # insert transaction
            self.insert_transaction(row, new_user_uuid)

        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e):
                print("User already exists")

                # retrieve user with that phone number
                user = self.retrieve_from_db(stmt=select(User.uuid).where(
                    User.phoneNumber == row["agentPhoneNumber"]))

                old_user_uuid = user[0]["uuid"]

                # insert transaction
                self.insert_transaction(row, old_user_uuid)
            else:
                print(f"Error:{e}")

        finally:
            session.close()

    def insert_transaction(self, row: pd.Series, user_uuid: uuid.uuid4):
        try:
            factory = sessionmaker(bind=self.pg_engine)
            session = factory()

            # insert transaction
            new_transaction = Transaction(mobile=row["receiverPhoneNumber"], commission=row["commission"],
                                          balance=row["balance"], amount=row["amount"], externalId=row["externalId"],
                                          requestTimestamp=row["date"], updateTimestamp=row["date"],
                                          userUuid=user_uuid)

            session.add(new_transaction)
            session.commit()

        except Exception as e:
            print(f"Error:{e}")

        finally:
            session.close()
