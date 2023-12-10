## Spark Implementation Documentation
### Configration File
- External congfigration json file include all the needed paramters, you need to change the valus to suit inside your enveroment and to be pass the file to the spark submit command.
  
### Description of the modules and libraries in the code
- The pyspark module includes the SparkContext and SQLContext classes.

- The google.cloud library is typically used for interacting with Google Cloud services, and specifically, the storage module is used for working with Google Cloud Storage. 

- pyspark.sql.functions contains functions to work with Spark DataFrames, including common SQL functions, i use col,ceil,dayofyear,lit,withColumn,orderBy

- pyspark.sql.types contains classes representing data types in Spark, useful when working with structured data, i use IntegerType.

- date, datetime, and timedelta classes from the datetime module in Python. These classes are commonly used for working with dates and times.

- json module, which is a standard module in Python for encoding and decoding JSON data.

### Dataproc Configuration
- gcloud dataproc clusters create cluster-name \
  --master-machine-type=c2-standard-30 \
  --worker-machine-type=c2-standard-30 \
  --master-boot-disk-type=pd-balanced \
  --master-boot-disk-size=500GB \
  --region=region \
  --num-workers=10
