"""
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import FloatType, DateType, TimestampType
from shapely import from_wkt


PROJECT_ROOT_PATH = os.environ.get("PROJECT_ROOT_PATH")
EXECUTION_DATE = os.environ.get("EXECUTION_DATE")
POSTGRES_JAR_PATH = os.environ.get("POSTGRES_JAR_PATH")

DATADIR_PATH = f"{PROJECT_ROOT_PATH}/data"
DICTIONARY_PATH = f"{DATADIR_PATH}/dictionary.csv"
CSVFILE_PATH = f"{DATADIR_PATH}/Fire_Incidents_{EXECUTION_DATE}.csv"

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PWD"),
    "driver": "org.postgresql.Driver"
}

DB_TABLES = {
    "raw": "landing_zone.fire_incidents"
}


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

    dictionary_df = spark_session.read \
                                 .options(delimiter=",", header=True) \
                                 .csv(dict_path) \
                                 .drop("Definition") \
                                 .drop("Notes") \
                                 .withColumnRenamed("Field Name", "name") \
                                 .withColumnRenamed("Data Type", "type") 

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


def save_to_raw_table(spark_session, dataframe):
    dataframe.write \
             .option("stringtype", "unspecified") \
             .jdbc(
                 DB_URL,
                 DB_TABLES["raw"],
                 mode="append", # mode="overwrite",
                 properties=DB_PROPERTIES
             )

def get_wkt_point(wkt_point):
    point = from_wkt(wkt_point)
    if point:
        return f"({point.x}, {point.y})"
    return None

wkt_point_udf = udf(get_wkt_point, returnType=StringType())


spark_session = SparkSession.builder \
                            .appName("sf-fire") \
                            .config("spark.jars", f"{POSTGRES_JAR_PATH}") \
                            .getOrCreate()

schema = get_schema(spark_session, DICTIONARY_PATH)
dataframe = get_csv(spark_session, CSVFILE_PATH, schema=schema) # enforcing schema
dataframe = dataframe.withColumn('point', wkt_point_udf(dataframe.point))
save_to_raw_table(spark_session, dataframe)

# # testing
# dataframe_columns = dataframe.columns
# first_row = dataframe.first().asDict()
# print()
# dataframe.printSchema()
# print()
# print(dataframe_columns)
# print()
# for key, value in first_row.items():
#     print(f'"{key}": {value} ({type(value)})')
# print()

spark_session.stop()
