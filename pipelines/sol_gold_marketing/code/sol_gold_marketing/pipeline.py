from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_gold_marketing.config.ConfigStore import *
from sol_gold_marketing.udfs.UDFs import *
from prophecy.utils import *
from sol_gold_marketing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_silver_dim_campaign = silver_dim_campaign(spark)
    df_reformat_campaign_data = reformat_campaign_data(spark, df_silver_dim_campaign)
    df_campaign_metrics_by_quarter = campaign_metrics_by_quarter(spark, df_reformat_campaign_data)
    df_campaign_performance = campaign_performance(spark, df_campaign_metrics_by_quarter)
    sol_gold_quarterly_campaign_revenue(spark, df_campaign_performance)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sol_gold_marketing")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/sol_gold_marketing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
