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


#filtering data based on condition
airbnb_data_filtered = airbnb_data.filter((airbnb_data['price']>5000) & (airbnb_data['review_scores_value']==10))


#calculating avg number of bathrooms and bedrooms
airbnb_data_agg = airbnb_data_filtered.select(fn.avg('bathrooms'), fn.avg('bedrooms'))


#renaming columns
airbnb_data_agg_renamed = airbnb_data_agg.withColumnRenamed('avg(bathrooms)','avg_bathrooms') \
                       .withColumnRenamed('avg(bedrooms)','avg_bedrooms')


#checking ouput
airbnb_data_agg_renamed.collect()


#Converting output into dataframe and writing into csv
airbnb_data_agg_renamed.toPandas().to_csv("out_part-2_task-3.txt", index=False)