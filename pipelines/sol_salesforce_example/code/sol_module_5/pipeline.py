from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sol_module_5.config.ConfigStore import *
from sol_module_5.udfs.UDFs import *
from prophecy.utils import *
from sol_module_5.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sol_salesforce_accounts = sol_salesforce_accounts(spark)
    df_reformat_account_data = reformat_account_data(spark, df_sol_salesforce_accounts)
    df_sol_salesforce_opportunities = sol_salesforce_opportunities(spark)
    df_reformat_lead_data = reformat_lead_data(spark, df_sol_salesforce_opportunities)
    df_join_by_account_id = join_by_account_id(spark, df_reformat_account_data, df_reformat_lead_data)
    df_revenue_by_account_and_lead = revenue_by_account_and_lead(spark, df_join_by_account_id)
    sol_module_5_revenue_report(spark, df_revenue_by_account_and_lead)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sol_salesforce_example")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/sol_salesforce_example", config = Config)(pipeline)

if __name__ == "__main__":
    main()
