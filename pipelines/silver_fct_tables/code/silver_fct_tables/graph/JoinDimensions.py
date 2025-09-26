from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_fct_tables.config.ConfigStore import *
from silver_fct_tables.udfs.UDFs import *

def JoinDimensions(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((col("in0.customer_id") == col("in1.customer_id")) & ((col("in1.valid_from") <= to_date(col("in0.sale_date"))) | col("in1.valid_from").isNull()))
            & ((col("in1.valid_to") > to_date(col("in0.sale_date"))) | col("in1.valid_to").isNull())
          ),
          "inner"
        )\
        .join(in2.alias("in2"), (col("in0.product_id") == col("in2.product_id")), "inner")\
        .select(col("in0.customer_id").alias("customer_id"), col("in0.order_id").alias("order_id"), col("in0.sale_date").alias("sale_date"), col("in1.valid_from").alias("valid_from"), col("in1.valid_to").alias("valid_to"), col("in0.product_id").alias("product_id"), col("in2.price_usd_cents").alias("price_usd_cents"), col("in1.phone_number").alias("shipping_phone"), concat_ws(
          ", ", 
          concat_ws(" ", col("in1.street_number"), col("in1.street_name"), col("in1.unit")), 
          col("in1.city"), 
          col("in1.state"), 
          col("in1.postcode")
        )\
        .alias("shipping_full_address"), col("in1.state").alias("shipping_state"))
