from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronzemarketingincremental.config.ConfigStore import *
from bronzemarketingincremental.udfs.UDFs import *

def raw_marketing_events_increment(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load(f"dbfs:/course_lab/rainforest_raw_data/marketing_events/{Config.dt}")
