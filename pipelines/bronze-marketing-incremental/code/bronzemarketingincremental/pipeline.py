from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bronzemarketingincremental.config.ConfigStore import *
from bronzemarketingincremental.udfs.UDFs import *
from prophecy.utils import *
from bronzemarketingincremental.graph import *

def pipeline(spark: SparkSession) -> None:
    df_raw_marketing_events_increment = raw_marketing_events_increment(spark)
    bronze_marketing_events_increment(spark, df_raw_marketing_events_increment)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/bronze-marketing-incremental")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/bronze-marketing-incremental", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
