from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def bronze_customers_increment(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted {}".format(f"`rainforest`.`{Config.bronze}`.`customers`"))
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    if table_exists:
        from delta.tables import DeltaTable, DeltaMergeBuilder
        updatesDF = in0.withColumn("is_original", lit("true")).withColumn("is_current", lit("true"))
        updateColumns = updatesDF.columns
        existingTable = DeltaTable.forName(spark, f"`rainforest`.`{Config.bronze}`.`customers`")
        existingDF = existingTable.toDF()
        cond = None
        scdHistoricColumns = ["signup_date",  "customer_name",  "phone_number",  "street_number",  "street_name",  "unit",  "city",                               "state",  "postcode",  "lat",  "lon"]

        for scdCol in scdHistoricColumns:
            if cond is None:
                cond = (~ (col("existingDF." + scdCol)).eqNullSafe(col("updatesDF." + scdCol)))
            else:
                cond = (cond | (~ (col("existingDF." + scdCol)).eqNullSafe(col("updatesDF." + scdCol))))

        stagedUpdatesDF = updatesDF\
                              .alias("updatesDF")\
                              .join(existingDF.alias("existingDF"), ["customer_id"])\
                              .filter((col("existingDF.is_current") == lit("true")) & (cond))\
                              .select(*[col("updatesDF." + val) for val in updateColumns])\
                              .withColumn("is_original", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("customer_id")))
        updateCond = None

        for scdCol in scdHistoricColumns:
            if updateCond is None:
                updateCond = (~ (col("existingDF." + scdCol)).eqNullSafe(col("staged_updates." + scdCol)))
            else:
                updateCond = (
                    updateCond
                    | (~ (col("existingDF." + scdCol))\
                      .eqNullSafe(col("staged_updates." + scdCol)))
                )

        colsToInsert = [c for c in stagedUpdatesDF.columns if c != "mergeKey"]
        colsToInsertDict = {}

        for colName in colsToInsert:
            colsToInsertDict[colName] = "staged_updates." + colName

        existingTable\
            .alias("existingDF")\
            .merge(
              stagedUpdatesDF.alias("staged_updates"),
              concat(col("existingDF.customer_id")) == col("staged_updates.mergeKey")
            )\
            .whenMatchedUpdate(
              condition = (col("existingDF.is_current") == lit("true")) & updateCond,
              set = {
"is_current" : "false", "valid_to" : "staged_updates.valid_from"}
            )\
            .whenNotMatchedInsert(values = colsToInsertDict)\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").saveAsTable(f"`rainforest`.`{Config.bronze}`.`customers`")
