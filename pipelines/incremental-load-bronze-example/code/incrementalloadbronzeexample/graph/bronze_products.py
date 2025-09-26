from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def bronze_products(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.bronze}`.`products`")
