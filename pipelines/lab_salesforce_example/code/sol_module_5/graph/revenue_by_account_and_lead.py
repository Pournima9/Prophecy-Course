from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_5.config.ConfigStore import *
from sol_module_5.udfs.UDFs import *

def revenue_by_account_and_lead(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("account_type"), col("lead_source"))

    return df1.agg(sum(col("expected_revenue")).alias("expected_revenue"))
