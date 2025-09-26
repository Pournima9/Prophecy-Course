from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def bronze_customers(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.bronze}`.`customers`")
