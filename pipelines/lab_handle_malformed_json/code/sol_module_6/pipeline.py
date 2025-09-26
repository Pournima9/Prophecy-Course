from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *
from prophecy.utils import *
from sol_module_6.graph import *

def pipeline(spark: SparkSession) -> None:
    df_lab_malformed_products = lab_malformed_products(spark)
    df_select_product_fields = select_product_fields(spark, df_lab_malformed_products)
    df_filter_non_null_prices = filter_non_null_prices(spark, df_select_product_fields)
    bronze_cleaned_products(spark, df_filter_non_null_prices)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/lab_handle_malformed_json")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/lab_handle_malformed_json", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
