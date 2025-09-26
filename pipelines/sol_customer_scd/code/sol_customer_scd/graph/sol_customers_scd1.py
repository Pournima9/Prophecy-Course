from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_customer_scd.config.ConfigStore import *
from sol_customer_scd.udfs.UDFs import *

def sol_customers_scd1(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted {}".format(f"`rainforest`.`{Config.bronze}`.`customers_scd1`"))
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    if table_exists:
        from delta.tables import DeltaTable, DeltaMergeBuilder
        DeltaTable\
            .forName(spark, f"`rainforest`.`{Config.bronze}`.`customers_scd1`")\
            .alias("target")\
            .merge(in0.alias("source"), (col("source.customer_id") == col("target.customer_id")))\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").saveAsTable(f"`rainforest`.`{Config.bronze}`.`customers_scd1`")
