from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def AddSalesQtr(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("sales_id"), 
        concat(year(col("sale_date")), lit("-Q"), quarter(col("sale_date"))).alias("sales_qtr"), 
        col("customer_id"), 
        col("order_id"), 
        col("sale_date"), 
        col("product_id"), 
        col("shipping_full_address"), 
        col("shipping_state").alias("customer_state"), 
        col("price_usd_cents"), 
        col("name"), 
        col("main_category"), 
        col("sub_category"), 
        col("date_introduced")
    )
