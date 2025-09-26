from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def SumSales(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("product_id"))

    return df1.agg(
        avg(col("product_price_usd_cents")).alias("product_price_usd_cents"), 
        sum((col("product_price_usd_cents") * col("product_qty"))).alias("total_sales_usd_cents"), 
        sum(col("product_qty")).alias("sales_qty")
    )
