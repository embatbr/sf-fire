#!/bin/bash

# Run it once. This will destroy the database (if exists) and recreate it.


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


# Hardcoded credentials just to simplify the challenge.
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="sf_fire"
DB_USER="sf_fire"
DB_PWD="prMSohYm0GoY9bnIZvEAnE5Go"


psql -U postgres -c "DROP DATABASE IF EXISTS ${DB_NAME};"
psql -U postgres -c "DROP USER IF EXISTS ${DB_USER};"

psql -U postgres -c "CREATE USER ${DB_USER} WITH SUPERUSER ENCRYPTED PASSWORD '${DB_PWD}';"
psql -U postgres -c "CREATE DATABASE ${DB_NAME} WITH OWNER = ${DB_USER};"


# creating the raw table
PGPASSWORD=${DB_PWD} psql -U ${DB_USER} -d ${DB_NAME} -f ${PROJECT_ROOT_PATH}/src/raw_fire_incidents.sql
