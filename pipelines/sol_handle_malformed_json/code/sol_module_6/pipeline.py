from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_module_6.config.ConfigStore import *
from sol_module_6.udfs.UDFs import *
from prophecy.utils import *
from sol_module_6.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sol_malformed_products = sol_malformed_products(spark)
    df_reformatted_product_data = reformatted_product_data(spark, df_sol_malformed_products)
    df_filter_non_null_price = filter_non_null_price(spark, df_reformatted_product_data)
    sol_cleaned_products(spark, df_filter_non_null_price)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sol_handle_malformed_json")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/sol_handle_malformed_json", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
