#importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn


#Setting Path of file
path = "C:\Users\Mitul\OneDrive\Desktop\Truata\part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet"


#Creating SparkSession obj
spark = SparkSession.builder.appName("Reading Airbnb Data").getOrCreate()


#Reading parquet file
airbnb_data = spark.read.parquet(path)


#Finding airbnb price min, max and row count
airbnb_price_details = airbnb_data.select(fn.min('price'), fn.max('price'), fn.count('price'))


#Renaming columns
airbnb_price_details = airbnb_price_details.withColumnRenamed('min(price)','min_price') \
                       .withColumnRenamed('max(price)','max_price') \
                       .withColumnRenamed('count(price)', 'row_count')

#checking output
airbnb_price_details.collect()


#Converting output into dataframe and writing into csv
airbnb_price_details.toPandas().to_csv("out_part-2_task-2.txt", index=False)