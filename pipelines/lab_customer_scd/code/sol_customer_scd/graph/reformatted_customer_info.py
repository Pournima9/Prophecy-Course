from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_customer_scd.config.ConfigStore import *
from sol_customer_scd.udfs.UDFs import *

def reformatted_customer_info(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("customer_id"), col("signup_date"), col("customer_name"), col("phone_number"))
