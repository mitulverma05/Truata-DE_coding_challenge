#importing packages
import pyspark
from pyspark.sql import SparkSession


#Setting Path of file
path = "C:\Users\Mitul\OneDrive\Desktop\Truata\part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet"


#Creating SparkSession obj
spark = SparkSession.builder.appName("Reading Airbnb Data").getOrCreate()


#Reading parquet file
airbnb_data = spark.read.parquet(path)


#Reading data
airbnb_data.show()