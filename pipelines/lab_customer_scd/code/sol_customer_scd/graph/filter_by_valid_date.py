from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_customer_scd.config.ConfigStore import *
from sol_customer_scd.udfs.UDFs import *

def filter_by_valid_date(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (col("valid_from").isNull() | (col("valid_from") <= lit(Config.dt)))
          & (col("valid_to").isNull() | (col("valid_to") >= lit(Config.dt)))
        )
    )
