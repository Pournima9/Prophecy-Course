from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *

def Deduplicate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy(
              "customer_id", 
              "signup_date", 
              "customer_name", 
              "phone_number", 
              "street_number", 
              "street_name", 
              "unit", 
              "city", 
              "state", 
              "postcode", 
              "LAT", 
              "LON"
            )\
            .orderBy(col("valid_to").asc()))
        )\
        .withColumn(
          "count",
          count("*")\
            .over(Window.partitionBy(
            "customer_id", 
            "signup_date", 
            "customer_name", 
            "phone_number", 
            "street_number", 
            "street_name", 
            "unit", 
            "city", 
            "state", 
            "postcode", 
            "LAT", 
            "LON"
          ))
        )\
        .filter(col("row_number") == col("count"))\
        .drop("row_number")\
        .drop("count")
