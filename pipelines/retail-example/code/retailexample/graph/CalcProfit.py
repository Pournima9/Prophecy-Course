from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def CalcProfit(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("cost_usd_cents", (col("sales_qty") * col("avg_cost_usd_cents")))\
        .withColumn("sales_profit_usd_cents", (col("total_sales_usd_cents") - col("cost_usd_cents")))\
        .withColumn("excess_inventory", (col("inventory") - col("sales_qty")))\
        .withColumn("excess_inventory_cost_usd_cents", (col("excess_inventory") * col("avg_cost_usd_cents")))\
        .withColumn("profit_usd_cents", (col("sales_profit_usd_cents") - col("excess_inventory_cost_usd_cents")))
