from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def bronze_sales_increment(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("mergeSchema", True)\
        .option("replaceWhere", f"sale_date = '{Config.dt}'")\
        .mode("overwrite")\
        .partitionBy("sale_date")\
        .saveAsTable(f"`rainforest`.`{Config.bronze}`.`sales`")
