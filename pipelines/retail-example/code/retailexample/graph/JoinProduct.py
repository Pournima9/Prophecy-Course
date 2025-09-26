from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def JoinProduct(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.product_id") == col("in1.product_id")), "inner")\
        .select(col("in0.product_id").alias("product_id"), col("in0.product_price_usd_cents").alias("product_price_usd_cents"), col("in0.total_sales_usd_cents").alias("total_sales_usd_cents"), col("in0.sales_qty").alias("sales_qty"), col("in1.avg_cost_usd_cents").alias("avg_cost_usd_cents"), col("in1.product_name").alias("product_name"), col("in1.product_category").alias("product_category"), col("in1.inventory").alias("inventory"))
