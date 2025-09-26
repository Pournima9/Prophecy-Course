from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldsalesreports.config.ConfigStore import *
from goldsalesreports.udfs.UDFs import *

def JoinProductId(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.product_id") == col("in1.product_id")), "inner")\
        .select(col("in1.sales_id").alias("sales_id"), col("in1.customer_id").alias("customer_id"), col("in1.order_id").alias("order_id"), col("in1.sale_date").alias("sale_date"), col("in1.product_id").alias("product_id"), col("in1.shipping_full_address").alias("shipping_full_address"), col("in1.shipping_state").alias("shipping_state"), col("in1.price_usd_cents").alias("price_usd_cents"), col("in0.name").alias("name"), col("in0.main_category").alias("main_category"), col("in0.sub_category").alias("sub_category"), col("in0.date_introduced").alias("date_introduced"))
