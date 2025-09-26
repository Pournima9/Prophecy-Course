from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def CountCampaignFields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        countDistinct(col("campaign_id")).alias("distinct_campaign_id"), 
        count(col("campaign_id")).alias("campaign_id_count"), 
        sum(expr("if((product_id IS NULL), 1, 0)")).alias("product_id_null_count")
    )
