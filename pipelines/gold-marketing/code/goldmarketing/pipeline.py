from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *
from prophecy.utils import *
from goldmarketing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_silver_dim_campaign = silver_dim_campaign(spark)
    df_bronze_products = bronze_products(spark)
    df_JoinProductCats = JoinProductCats(spark, df_silver_dim_campaign, df_bronze_products)
    df_LosingMoney = LosingMoney(spark, df_JoinProductCats)
    df_GroupByRollup_1 = GroupByRollup_1(spark, df_JoinProductCats)
    df_CleanFields = CleanFields(spark, df_GroupByRollup_1)
    df_Filter_1 = Filter_1(spark, df_CleanFields)
    gold_marketing_cube(spark, df_CleanFields)
    df_SQLStatement_1 = SQLStatement_1(spark, df_JoinProductCats)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gold-marketing")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/gold-marketing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
