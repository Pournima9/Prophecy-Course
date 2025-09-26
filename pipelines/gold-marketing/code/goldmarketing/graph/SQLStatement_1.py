from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def SQLStatement_1(spark: SparkSession, in0: DataFrame) -> (DataFrame):

    try:
        registerUDFs(spark)
    except NameError:
        print("registerUDFs not working")

    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT main_category,\n       sub_category,\n       campaign_start,\n       count(*) AS num_campaigns,\n       sum(campaign_cost_micros_usd) AS cost_micros_usd,\n       sum(revenue_generated_micros_usd) AS revenue_generated_micros_usd\nFROM in0\nGROUP BY rollup(main_category, sub_category, campaign_start)"
    )

    return df1
