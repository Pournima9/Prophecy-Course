from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def silver_fct_sales(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`rainforest`.`{Config.silver}`.`fct_sales`")
