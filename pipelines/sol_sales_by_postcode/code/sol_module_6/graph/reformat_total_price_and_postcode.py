from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *

def reformat_total_price_and_postcode(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        (col("total_price_usd_cents") / lit(100)).cast(DecimalType(30, 2)).alias("total_sales_usd"), 
        col("postcode")
    )
