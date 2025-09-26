from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def bronze_products_increment(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"date_introduced = '{Config.dt}'")\
        .mode("overwrite")\
        .partitionBy("date_introduced")\
        .saveAsTable(f"`rainforest`.`{Config.bronze}`.`products`")
