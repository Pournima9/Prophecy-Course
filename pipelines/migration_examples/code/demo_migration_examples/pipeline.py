from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *
from prophecy.utils import *
from demo_migration_examples.graph import *

def pipeline(spark: SparkSession) -> None:
    DeltaTableOperations_1(spark)
    df_silver_dim_campaign_1 = silver_dim_campaign_1(spark)
    df_AddTS2 = AddTS2(spark, df_silver_dim_campaign_1)
    df_test_campaign_base_2 = test_campaign_base_2(spark)
    df_silver_dim_campaign_2 = silver_dim_campaign_2(spark)
    df_silver_dim_campaign = silver_dim_campaign(spark)
    df_test_campaign_base_1 = test_campaign_base_1(spark)
    df_MakeIncompatible_1 = MakeIncompatible_1(spark, df_test_campaign_base_1)
    test_campaign_overwrite(spark, df_MakeIncompatible_1)
    df_Limit_1 = Limit_1(spark, df_silver_dim_campaign)
    df_AddTS = AddTS(spark, df_Limit_1)
    test_campaign_base(spark, df_AddTS)
    df_silver_dim_campaign_3 = silver_dim_campaign_3(spark)
    df_Limit_2 = Limit_2(spark, df_silver_dim_campaign_3)
    df_MakeCompatibleChange = MakeCompatibleChange(spark, df_Limit_2)
    test_campaign_compatible(spark, df_MakeCompatibleChange)
    df_Limit_3 = Limit_3(spark, df_silver_dim_campaign_2)
    df_MakeIncompatible = MakeIncompatible(spark, df_Limit_3)
    test_campaign_incompatible(spark, df_MakeIncompatible)
    test_campaign_partitioned(spark, df_AddTS2)
    df_test_campaign_read_version = test_campaign_read_version(spark)
    df_test_campaign_read_timestamp = test_campaign_read_timestamp(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/migration_examples")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/migration_examples", config = Config)(pipeline)

if __name__ == "__main__":
    main()
