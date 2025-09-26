from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from retailexample.config.ConfigStore import *
from retailexample.udfs.UDFs import *

def FlattenOrders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.withColumn("ordered_products", explode_outer("ordered_products")).columns
    selectCols = [col("order_number") if "order_number" in flt_col else col("order_number"),                   col("ordered_products-id") if "ordered_products-id" in flt_col else col("ordered_products.id")\
                    .alias("ordered_products-id"),                   col("ordered_products-price") if "ordered_products-price" in flt_col else col("ordered_products.price")\
                    .alias("ordered_products-price"),                   col("ordered_products-qty") if "ordered_products-qty" in flt_col else col("ordered_products.qty")\
                    .alias("ordered_products-qty")]

    return in0.withColumn("ordered_products", explode_outer("ordered_products")).select(*selectCols)
