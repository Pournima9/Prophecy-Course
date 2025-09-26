from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_5.config.ConfigStore import *
from sol_module_5.udfs.UDFs import *

def reformat_lead_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("AccountId").alias("account_id"), 
        col("LeadSource").alias("lead_source"), 
        col("ExpectedRevenue").cast(LongType()).alias("expected_revenue")
    )
