#Importing Spark Module
from pyspark import SparkContext


#Creating Spark Context Object
sc = SparkContext.getOrCreate()


#Setting Path of file
path = "C:\Users\Mitul\OneDrive\Desktop\Truata\groceries.csv"


#Creating RDD/Reading CSV using textfile
Groceries_List = sc.textFile(path)


#Using flatmap to create new rdd for every word using comma
Groceries_List = Groceries_List.flatMap(lambda x: x.split(","))


#using map to generate key, value pair for every rdd
Groceries_List = Groceries_List.map(lambda x : (x,1))


#using reducebykey to merge values for each key using specified function
Groceries_List = Groceries_List.reduceByKey(lambda a,b: a+b)


#uisng map to tranformation data into (value, key) and then Sorting into descending order
Groceries_List = Groceries_List.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)


#Reading top 5 records
Groceries_List.take(5)


#Writing data in ouput file
with open('out_part-1_task-1_3.txt', 'w') as f:
    for line in Groceries_List.take(5):
        f.write("{}".format(line))
        f.write('\n')