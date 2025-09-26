from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_fct_tables.config.ConfigStore import *
from silver_fct_tables.udfs.UDFs import *

def FormatSales(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        shiftRightUnsigned(xxhash64(col("order_id"), col("product_id"), col("sale_date")), 1).alias("sales_id"), 
        col("customer_id"), 
        col("order_id"), 
        col("sale_date"), 
        col("product_id"), 
        col("price_usd_cents"), 
        col("shipping_phone"), 
        col("shipping_full_address"), 
        col("shipping_state")
    )
