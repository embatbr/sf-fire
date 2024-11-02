"""
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import FloatType, DateType, TimestampType


PROJECT_ROOT_PATH = os.environ.get("PROJECT_ROOT_PATH")
EXECUTION_DATE = os.environ.get("EXECUTION_DATE")

DATADIR_PATH = f"{PROJECT_ROOT_PATH}/data"
DICTIONARY_PATH = f"{DATADIR_PATH}/dictionary.csv"
CSVFILE_PATH = f"{DATADIR_PATH}/Fire_Incidents_{EXECUTION_DATE}.csv"
# CSVFILE_PATH = f"{DATADIR_PATH}/sample.csv"


def get_struct_type(row_type):
    if row_type == "Text":
        return StringType
    if row_type == "Integer":
        return IntegerType
    if row_type == "Numeric":
        return FloatType
    if row_type == "Date":
        return DateType
    if row_type == "Date & Time":
        return TimestampType
    return StringType


def get_schema(spark_session, dict_path):
    """Returns a schema given a data dictionary filepath.
    """

    dictionary_df = (
        spark_session.read
                     .options(delimiter=",", header=True)
                     .csv(dict_path)
                     .drop("Definition")
                     .drop("Notes")
                     .withColumnRenamed("Field Name", "name") # for future syntactic sugar
                     .withColumnRenamed("Data Type", "type") # for future syntactic sugar
    )

    return StructType(dictionary_df.rdd.map(
        lambda row: StructField(row.name, get_struct_type(row.type)(), True)
    ).collect())


def get_csv(spark_session, csv_path, schema=None):
    df_reader = spark_session.read

    if schema:
        df_reader = df_reader.schema(schema)

    df_reader = df_reader.options(
        delimiter=",",
        header=True,
        dateFormat="yyyy/MM/dd",
        timestampFormat="yyyy/MM/dd hh:mm:ss a"
    )
    
    return df_reader.csv(csv_path)


spark_session = SparkSession.builder.appName("sf-fire").getOrCreate()

schema = get_schema(spark_session, DICTIONARY_PATH)
csv_df = get_csv(spark_session, CSVFILE_PATH, schema=schema)

# testing
csv_df_columns = csv_df.columns
first_row = csv_df.first().asDict()
print()
csv_df.printSchema()
print()
print(csv_df_columns)
print()
for key, value in first_row.items():
    print(f'"{key}": {value} ({type(value)})')
print()

spark_session.stop()
