from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def raw_customer_snapshot(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("customer_id", LongType(), True), StructField("signup_date", DateType(), True), StructField("customer_name", StringType(), True), StructField("phone_number", StringType(), True), StructField("street_number", StringType(), True), StructField("street_name", StringType(), True), StructField("unit", StringType(), True), StructField("city", StringType(), True), StructField("state", StringType(), True), StructField("postcode", StringType(), True), StructField("lat", DoubleType(), True), StructField("lon", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv(f"{Config.source_path}/customer_snapshots/{Config.dt}")
