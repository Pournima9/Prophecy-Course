from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sol_module_5.config.ConfigStore import *
from sol_module_5.udfs.UDFs import *

def sol_salesforce_accounts(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Id", StringType(), True), StructField("IsDeleted", StringType(), True), StructField("MasterRecordId", StringType(), True), StructField("Name", StringType(), True), StructField("Type", StringType(), True), StructField("ParentId", StringType(), True), StructField("BillingStreet", StringType(), True), StructField("BillingCity", StringType(), True), StructField("BillingState", StringType(), True), StructField("BillingPostalCode", StringType(), True), StructField("BillingCountry", StringType(), True), StructField("BillingLatitude", StringType(), True), StructField("BillingLongitude", StringType(), True), StructField("BillingGeocodeAccuracy", StringType(), True), StructField("ShippingStreet", StringType(), True), StructField("ShippingCity", StringType(), True), StructField("ShippingState", StringType(), True), StructField("ShippingPostalCode", StringType(), True), StructField("ShippingCountry", StringType(), True), StructField("ShippingLatitude", StringType(), True), StructField("ShippingLongitude", StringType(), True), StructField("ShippingGeocodeAccuracy", StringType(), True), StructField("Phone", StringType(), True), StructField("Fax", StringType(), True), StructField("AccountNumber", StringType(), True), StructField("Website", StringType(), True), StructField("Sic", StringType(), True), StructField("Industry", StringType(), True), StructField("AnnualRevenue", StringType(), True), StructField("NumberOfEmployees", StringType(), True), StructField("Ownership", StringType(), True), StructField("TickerSymbol", StringType(), True), StructField("Description", StringType(), True), StructField("Rating", StringType(), True), StructField("Site", StringType(), True), StructField("OwnerId", StringType(), True), StructField("CreatedDate", StringType(), True), StructField("CreatedById", StringType(), True), StructField("LastModifiedDate", StringType(), True), StructField("LastModifiedById", StringType(), True), StructField("SystemModstamp", StringType(), True), StructField("LastActivityDate", StringType(), True), StructField("Jigsaw", StringType(), True), StructField("JigsawCompanyId", StringType(), True), StructField("CleanStatus", StringType(), True), StructField("AccountSource", StringType(), True), StructField("DunsNumber", StringType(), True), StructField("Tradestyle", StringType(), True), StructField("NaicsCode", StringType(), True), StructField("NaicsDesc", StringType(), True), StructField("YearStarted", StringType(), True), StructField("SicDesc", StringType(), True), StructField("DandbCompanyId", StringType(), True), StructField("OperatingHoursId", StringType(), True), StructField("CustomerPriority__c", StringType(), True), StructField("SLA__c", StringType(), True), StructField("Active__c", StringType(), True), StructField("NumberofLocations__c", StringType(), True), StructField("UpsellOpportunity__c", StringType(), True), StructField("SLASerialNumber__c", StringType(), True), StructField("SLAExpirationDate__c", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/salesforce_export/Account.csv")
