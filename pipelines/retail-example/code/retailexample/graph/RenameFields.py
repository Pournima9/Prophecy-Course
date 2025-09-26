from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def RenameFields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("order_number"), 
        col("`ordered_products-id`").alias("product_id"), 
        (lit(100) * col("`ordered_products-price`")).cast(LongType()).alias("product_price_usd_cents"), 
        col("`ordered_products-qty`").alias("product_qty")
    )
