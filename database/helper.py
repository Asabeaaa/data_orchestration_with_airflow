from sqlalchemy.orm import sessionmaker
from database.schema import Transaction, User
from database.base import Connector


class UpdateDB(Connector):
    def update_pg_db(self, row):
        try:
            factory = sessionmaker(bind=self.pg_engine)
            session = factory()

            # insert new user
            new_user = User(
                phoneNumber=row["agentPhoneNumber"], nTransactions=0)
            session.add(new_user)
            session.commit()

        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e):
                print("User already exists")
