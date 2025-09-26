from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *

def writeto_quarterly_campaign_revenue(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .saveAsTable("`rainforest`.`bronze_c3782efe`.`quarterly_campaign_revenue`")
