from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_fct_tables.config.ConfigStore import *
from silver_fct_tables.udfs.UDFs import *

def FlattenProducts(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.withColumn("products", explode_outer("products")).columns
    selectCols = [col("customer_id") if "customer_id" in flt_col else col("customer_id"),                   col("order_id") if "order_id" in flt_col else col("order_id"),                   col("sale_date") if "sale_date" in flt_col else col("sale_date"),                   col("product_id") if "product_id" in flt_col else col("products.product_id").alias("product_id")]

    return in0.withColumn("products", explode_outer("products")).select(*selectCols)
