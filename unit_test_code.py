from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import date
from datetime import datetime
from datetime import timedelta
data1 = [(1,"x","2002","xtec","xmanf","xmodel"),
(2,"y","2005","ytec","ymanf","ymodel"),
(3,"z","2022","ztec","zmanf","zmodel"),
(4,"c","2003","ctec","cmanf","cmodel"),
(5,"d","2012","tec","dmanf","dmodel")
  ]

schema = StructType([ \
    StructField("device_tac",IntegerType(),True), \
    StructField("device_type",StringType(),True), \
    StructField("device_live_since_date",StringType(),True), \
    StructField("device_supported_technologies", StringType(), True), \
    StructField("device_manufacturer", StringType(), True), \
    StructField("device_model", StringType(), True) \
  ])
 
source_1= spark.createDataFrame(data=data1,schema=schema)

data2 = [(1,"2023-12-01 00:00:00",2,10),
(1,"2023-12-09 00:00:00",2,10),
(2,"2023-12-08 00:00:00",3,15),
(3,"2023-12-10 00:00:00",4,8),
(4,"2023-12-11 00:00:00",4,16),
(5,"2023-12-11 00:00:00",5,15)
  ]

schema_source_2 = StructType([ \
    StructField("device_tac",IntegerType(),True), \
    StructField("date_time_stamp",StringType(),True), \
    StructField("energyconsumption",IntegerType(),True), \
    StructField("trafficlevel", IntegerType(), True)
  ])

source_2= spark.createDataFrame(data=data2,schema=schema_source_2)
source_2 = source_2.withColumn('date', date_format(col('date_time_stamp'), 'yyyy-MM-dd'))

start_date = date.today().strftime("%Y-%m-%d")
end_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(7)).strftime("%Y-%m-%d")

source_2.createOrReplaceTempView("radio_wizard_table")

sql_query = """
    SELECT * FROM radio_wizard_table
    WHERE date BETWEEN '{}' AND '{}'
""".format(end_date, start_date)

week_data = spark.sql(sql_query)

week_data = week_data.withColumn('week', ceil(dayofyear(lit(start_date)) / 7))
final_source_df = source_1.join(week_data, "device_tac")
final_source_df = final_source_df.withColumn('performance', col('trafficlevel') / col('energyconsumption'))
final_source_df = final_source_df.orderBy("performance").limit(5)
final_source_df = final_source_df.select("week", "device_tac","device_manufacturer","energyconsumption", "trafficlevel", "performance")

#The expected device_tac order is 3,5,4,1,2
final_source_df.show()
