from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_fct_tables.config.ConfigStore import *
from silver_fct_tables.udfs.UDFs import *

def FormatOrders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("order_id"), col("customer_id"), col("sale_date"))

    return df1.agg(
        sum(col("price_usd_cents")).alias("total_price_usd_cents"), 
        first(col("shipping_phone")).alias("shipping_phone"), 
        first(col("shipping_full_address")).alias("shipping_full_address"), 
        first(col("shipping_state")).alias("shipping_state")
    )
