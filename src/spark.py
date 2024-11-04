"""
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
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
    "raw": "landing.fire_incidents",
    "dimensions": {
        "Incident Date": {
            "table": "business.dim_incident_dates",
            "table_column": "incident_date",
            "fk_column": "incident_date_id"
        },
        "Battalion": {
            "table": "business.dim_battalions",
            "table_column": "battalion",
            "fk_column": "battalion_id"
        },
        "Supervisor District": {
            "table": "business.dim_supervisor_districts",
            "table_column": "supervisor_district",
            "fk_column": "supervisor_district_id"
        },
        "neighborhood_district": {
            "table": "business.dim_neighborhood_districts",
            "table_column": "neighborhood_district",
            "fk_column": "neighborhood_district_id"
        }
    }
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


def write_to_table(spark_session, dataframe, table):
    dataframe.write \
             .option("stringtype", "unspecified") \
             .jdbc(
                 DB_URL,
                 table,
                 mode="append", # mode="overwrite",
                 properties=DB_PROPERTIES
             )


def read_from_table(spark_session, table):
    return spark_session.read \
                        .jdbc(
                            DB_URL,
                            table,
                            properties=DB_PROPERTIES
                        )


def get_wkt_point(wkt_point):
    point = from_wkt(wkt_point)
    if point:
        return f"({point.x}, {point.y})"
    return None

wkt_point_udf = udf(get_wkt_point, returnType=StringType())


def create_dim_table(spark_session, dataframe, column):
    dim_table = DB_TABLES["dimensions"][column]["table"]
    dim_table_column = DB_TABLES["dimensions"][column]["table_column"]
    fact_fk_column = DB_TABLES["dimensions"][column]["fk_column"]

    dim_dataframe = dataframe.select(
                                 col(column).alias(dim_table_column)
                             ) \
                             .distinct() \
                             .where(
                                 col(column).isNotNull()
                             )
    write_to_table(spark_session, dim_dataframe, dim_table) # gonna break when running for several days
    dim_dataframe = read_from_table(spark_session, dim_table) # just to get the PK

    dataframe = dataframe.join(
        dim_dataframe,
        dataframe[column] == dim_dataframe[dim_table_column],
        "left_outer"
    )
    dataframe = dataframe.drop(column)
    dataframe = dataframe.drop(dim_table_column)
    dataframe = dataframe.withColumnRenamed("_table_id", fact_fk_column)

    return dataframe


def rename_all_columns(dataframe):
    columns = dataframe.columns

    # this loop is suboptimal, but...
    for column in columns:
        new_column = column.lower().replace(' ', '_')
        dataframe = dataframe.withColumnRenamed(column, new_column)

    dataframe = dataframe.withColumnRenamed("box", "city_box")
    dataframe = dataframe.withColumnRenamed("point", "point_location")

    return dataframe


spark_session = SparkSession.builder \
                            .appName("sf-fire") \
                            .config("spark.jars", f"{POSTGRES_JAR_PATH}") \
                            .getOrCreate()

schema = get_schema(spark_session, DICTIONARY_PATH)
dataframe = get_csv(spark_session, CSVFILE_PATH, schema=schema) # enforcing schema
dataframe = dataframe.withColumn('point', wkt_point_udf(dataframe.point))
write_to_table(spark_session, dataframe, DB_TABLES["raw"])
dataframe = create_dim_table(spark_session, dataframe, "Incident Date")
dataframe = create_dim_table(spark_session, dataframe, "Battalion")
dataframe = create_dim_table(spark_session, dataframe, "Supervisor District")
dataframe = create_dim_table(spark_session, dataframe, "neighborhood_district")
dataframe = rename_all_columns(dataframe)
write_to_table(spark_session, dataframe, "business.fact_fire_incidents")

# print()
# dataframe.printSchema()
# print()

spark_session.stop()
