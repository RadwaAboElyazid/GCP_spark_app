#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#imports
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date
from datetime import datetime
from datetime import timedelta
import json


#Read the configraion file
with open('project_config.json') as f:
    data = json.load(f)

    
#spark session

spark = SparkSession.builder.appName("task_2").getOrCreate()


path_to_your_credentials_json = data["path_to_your_credentials_json"]
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",path_to_your_credentials_json)



#read_source_1
bucket_name=data['bucket_name']
sourc_1 = data['GSMA_device_source']
path=f"gs://{bucket_name}/{sourc_1}"
GSMA_device_GCS_df = spark.read.options(header='True',delimiter=';').csv(path)

#convert the Pk to intger to prepair it to join step or i can inferSchema while read
GSMA_device_GCS_df = GSMA_device_GCS_df.withColumn("device_tac",GSMA_device_GCS_df.device_tac.cast(IntegerType()))


#read_bigquery_Table_source_2
project_name = data["project_name"]
dataset_name = data["dataset_name"]
table_name = data["radio_wizard_source"]
radio_wizard_BQ_df = spark.read.format("bigquery").option("project", project_name).option("dataset", dataset_name).option("table", table_name).load()
radio_wizard_BQ_df = radio_wizard_BQ_df.withColumn('date', date_format(col('date_time_stamp'), 'yyyy-MM-dd'))
#week_duration
start_date = date.today().strftime("%Y-%m-%d")
end_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(7)).strftime("%Y-%m-%d")


radio_wizard_BQ_df.createOrReplaceTempView("radio_wizard_table")

sql_query = """
    SELECT * FROM radio_wizard_table
    WHERE date BETWEEN '{}' AND '{}'
""".format(end_date, start_date)

radio_wizard_df = spark.sql(sql_query)
radio_wizard_df = radio_wizard_df.withColumn('week', ceil(dayofyear(lit(start_date)) / 7))

#inner join the tow sources
final_source_df = GSMA_device_GCS_df.join(radio_wizard_df, "device_tac")

### NOTE ###
"""
#I assumed that the daily updates in the BigQuery source affect the same records,
so there is no need for any aggregation on the 'energyconsumption' or 
'trafficlevel' columns per device_tac per week.
#If we have the other assumption that we have multiple insertion 
per device_tac we will need extra
transformation step to aggregate 
energyconsumption' and 'trafficlevel' columns per device_tac per week.
"""
#calculate the performance column
final_source_df = final_source_df.withColumn('performance', col('trafficlevel') / col('energyconsumption'))
final_source_df = final_source_df.orderBy("performance").limit(10)

#select the needed column for the output dataframe
final_source_df = final_source_df.select("week", "device_tac","device_manufacturer","energyconsumption", "trafficlevel", "performance")


#write the final result to GCS partion by the week column
sink_path = data["GCS_sink_path"]
#gcs_path = "gs://ten_worst_performing_devices/"
final_source_df.write.format("parquet").partitionBy("week").option("path", sink_path).save()

spark.stop()

