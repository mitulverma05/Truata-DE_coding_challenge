#importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, FloatType, StringType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline


#Setting up file path
path = "C:\Users\Mitul\OneDrive\Desktop\Truata\iris.data"


#Creating SparkSession obj
spark = SparkSession.builder.appName("Irish Data").getOrCreate()


#Defining data schema
schema = StructType() \
       .add('sepal_length',FloatType(),True)\
       .add('sepal_width', FloatType(), True) \
       .add('petal_length', FloatType(), True) \
       .add('petal_width', FloatType(), True) \
       .add('class', StringType(), True)


#Reading data
irish_data = spark.read.csv(path, schema=schema)


#Replace the Dependent variable with numeric value
classindex = StringIndexer(inputCol = 'class', outputCol = 'classIndex')
final_data = classindex.fit(irish_data).transform(irish_data)


#Creating a class label for future comparison
class_label = final_data.select("classIndex", "class").distinct()


# vectorize all numerical columns into a single feature column
assembler = VectorAssembler(inputCols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], outputCol = 'features')


#Creating logistic Regression obj
lr = LogisticRegression(featuresCol = 'features', labelCol = 'classIndex')


#Creating a pipeline to execute process in order
pipeline = Pipeline(stages = [assembler, lr])


#Training model
model_train = pipeline.fit(final_data)


#Defining test values
predicted_data = spark.createDataFrame([(5.1, 3.5, 1.4, 0.2),(6.2, 3.4, 5.4, 2.3)],["sepal_length", "sepal_width", "petal_length", "petal_width"])


#Testing model
result = model_train.transform(predicted_data).select('prediction')


#Combining result
prediction = result.join(class_label, class_label['classIndex'] == result['prediction']).select('class')


#Saving data to txt file
prediction.toPandas().to_csv('out_3_2.txt', index= False)