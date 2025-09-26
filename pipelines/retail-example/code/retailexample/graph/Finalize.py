from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def Finalize(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("product_name"), 
        (col("profit_usd_cents") / lit(100)).cast(DecimalType(20, 2)).alias("profit_usd"), 
        (col("excess_inventory_cost_usd_cents") / lit(100)).cast(DecimalType(20, 2)).alias("excess_inventory_cost_usd")
    )
