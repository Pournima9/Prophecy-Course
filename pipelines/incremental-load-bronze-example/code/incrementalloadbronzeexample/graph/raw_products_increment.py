from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def raw_products_increment(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load(f"{Config.source_path}/products_parquet/{Config.dt}")
