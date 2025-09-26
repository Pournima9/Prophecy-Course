from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_5.config.ConfigStore import *
from sol_module_5.udfs.UDFs import *

def join_by_account_id(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.account_id") == col("in1.account_id")), "inner")\
        .select(col("in0.account_id").alias("account_id"), col("in0.account_type").alias("account_type"), col("in1.lead_source").alias("lead_source"), col("in1.expected_revenue").alias("expected_revenue"))
