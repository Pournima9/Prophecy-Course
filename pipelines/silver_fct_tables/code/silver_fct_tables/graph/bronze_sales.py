from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_fct_tables.config.ConfigStore import *
from silver_fct_tables.udfs.UDFs import *

def bronze_sales(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.bronze}`.`sales`")
