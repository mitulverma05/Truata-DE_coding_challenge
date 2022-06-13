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


#selecting lowest price and hightest ratings
lowestprice = airbnb_data.select(fn.min('price')).collect()[0][0]
highestrating = airbnb_data.select(fn.max('review_scores_rating')).collect()[0][0]


#filtering data based on condition
airbnb_data_filtered = airbnb_data.filter((airbnb_data['price']==lowestprice) & (airbnb_data['review_scores_rating']==highestrating))


#Selecting desired column
airbnb_data_filtered.select('accommodates').collect()[0][0]


#Writing output into txt file
with open('out_part-2_task-4.txt', 'w') as f:
        f.write("number of people: {}".format(airbnb_data_filtered.select('accommodates').collect()[0][0]))