from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bulkloadbronzeexample.config.ConfigStore import *
from bulkloadbronzeexample.udfs.UDFs import *

def bronze_sales(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .partitionBy("sale_date")\
        .saveAsTable(f"`rainforest`.`{Config.bronze}`.`sales`")
