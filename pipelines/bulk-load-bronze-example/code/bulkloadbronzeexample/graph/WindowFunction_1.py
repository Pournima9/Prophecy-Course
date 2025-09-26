from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *

def WindowFunction_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "valid_ahead",
          expr("lead(valid_to)").over(Window.partitionBy(col("customer_id")).orderBy(col("valid_to").asc()))
        )\
        .withColumn("valid_from", expr("lag(valid_to)").over(Window.partitionBy(col("customer_id")).orderBy(col("valid_to").asc())))
