from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *
from prophecy.utils import *
from dataquality.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_customers = bronze_customers(spark)
    df_silver_fct_sales = silver_fct_sales(spark)
    df_JoinCustomerId = JoinCustomerId(spark, df_silver_fct_sales, df_bronze_customers)
    df_CountMissing = CountMissing(spark, df_JoinCustomerId)
    df_FormatCustomerIdError = FormatCustomerIdError(spark, df_CountMissing)
    df_CountFields = CountFields(spark, df_silver_fct_sales)
    df_FormatErrors = FormatErrors(spark, df_CountFields)
    df_Unnest = Unnest(spark, df_FormatErrors)
    df_CustomersEarly = CustomersEarly(spark, Config.CustomersEarly)
    df_SignupFilterEarly = SignupFilterEarly(spark, df_CustomersEarly)
    df_CustomersLater = CustomersLater(spark, Config.CustomersLater)
    df_SignupFilterLate = SignupFilterLate(spark, df_CustomersLater)
    df_CompareDates = CompareDates(spark, df_SignupFilterEarly, df_SignupFilterLate)
    df_SelectSignupDate = SelectSignupDate(spark, df_CompareDates)
    df_FormatSignupError = FormatSignupError(spark, df_SelectSignupDate)
    df_UnionErrors = UnionErrors(spark, df_Unnest, df_FormatCustomerIdError, df_FormatSignupError)
    df_UnionErrors = df_UnionErrors.cache()
    df_AddCheckTime = AddCheckTime(spark, df_UnionErrors)
    sales_dq_checks(spark, df_AddCheckTime)
    df_Union = Union(spark, df_CustomersEarly, df_CustomersLater)
    df_SelectCustomer = SelectCustomer(spark, df_Union)
    df_FilterErrors = FilterErrors(spark, df_UnionErrors)
    Assert(spark, df_FilterErrors)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/data-quality")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/data-quality", config = Config)(pipeline)

if __name__ == "__main__":
    main()
