#!/bin/bash
set -e
echo "DATABASE INIT: creating postgres user and database for app"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $PG_USER WITH PASSWORD '$PG_PW' SUPERUSER;
    CREATE DATABASE $PG_DBNAME;
    GRANT ALL PRIVILEGES ON DATABASE $PG_DBNAME TO $PG_USER;
EOSQL

echo "DATABASE INIT: initializing tables for $PG_DBNAME"
psql -v ON_ERROR_STOP=1 --username $PG_USER --dbname $PG_DBNAME <<-EOSQL
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE public."user"
(
    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
    "phoneNumber" character varying,
    "createdAt" timestamp without time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp without time zone NOT NULL DEFAULT now(),
    "nTransactions" bigint, -- number of transactions the agent has done
    CONSTRAINT "PK_a95e949168be7b7ece1a2382fed" PRIMARY KEY (uuid),
    CONSTRAINT "UQ_f2578043e491921209f5dadd080" UNIQUE ("phoneNumber")
);

CREATE TABLE transaction
(
    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
    mobile character varying, -- phone number of the client
    status character varying,
    category character varying, -- transaction type, e.g. cashin/cashout etc
    "userUuid" uuid NOT NULL,
    balance numeric(20,2), -- agent balance AFTER the transaction
    commission numeric(20,2),
    amount numeric(20,2),
    "requestTimestamp" bigint NOT NULL, -- unix timestamp in milliseconds
    "updateTimestamp" bigint NOT NULL, -- unix timestamp in milliseconds
    source character varying, 
    "externalId" character varying,
    CONSTRAINT "PK_fcce0ce5cc7762e90d2cc7e2307" PRIMARY KEY (uuid),
    CONSTRAINT "FK_00197c2fde23b7c0f6b69d0b6a2" FOREIGN KEY ("userUuid")
        REFERENCES public."user" (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
EOSQL