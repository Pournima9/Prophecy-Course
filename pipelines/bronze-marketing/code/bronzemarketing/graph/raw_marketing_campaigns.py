from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronzemarketing.config.ConfigStore import *
from bronzemarketing.udfs.UDFs import *

def raw_marketing_campaigns(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .schema(
          StructType([
            StructField("campaign_end", StringType(), True), StructField("campaign_id", LongType(), True), StructField("campaign_start", StringType(), True), StructField("product_id", LongType(), True)
        ])
        )\
        .load("dbfs:/course_lab/rainforest_raw_data/marketing_campaigns/*")
