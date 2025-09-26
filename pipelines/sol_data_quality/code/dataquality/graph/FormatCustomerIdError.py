from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def FormatCustomerIdError(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        (col("missing_customer_id_count") > lit(0)).alias("is_error"), 
        lit("there are customer_ids in sales table not found in customer table").alias("error_message")
    )
