from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def AvgCost(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("EAN13"), col("EAN5"))

    return df1.agg(
        round((sum((col("price_usd_cents") * col("quantity"))) / sum(col("quantity"))))\
          .cast(IntegerType())\
          .alias("avg_cost_usd_cents"), 
        sum(col("quantity")).alias("inventory")
    )
