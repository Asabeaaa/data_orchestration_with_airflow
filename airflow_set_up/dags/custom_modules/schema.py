from sqlalchemy import Column, String, DateTime, BigInteger, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class User(Base):
    __tablename__ = 'user'

    uuid = Column(UUID(as_uuid=True), primary_key=True)
    phoneNumber = Column(String)
    createdAt = Column(DateTime, default=datetime.now())
    updatedAt = Column(DateTime, default=datetime.now())
    nTransactions = Column(BigInteger)


class Transaction(Base):
    __tablename__ = 'transaction'

    uuid = Column(UUID(as_uuid=True), primary_key=True)
    mobile = Column(String)
    status = Column(String)
    category = Column(String)
    userUuid = Column(UUID(as_uuid=True))
    balance = Column(Float)
    commission = Column(Float)
    amount = Column(Float)
    requestTimestamp = Column(BigInteger)
    updateTimestamp = Column(BigInteger)
    source = Column(String)
    externalId = Column(String)
