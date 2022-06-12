#Importing Spark Module
from pyspark import SparkContext


#Creating Spark Context Object
sc = SparkContext.getOrCreate()


#Setting Path of file
path = "C:\Users\Mitul\OneDrive\Desktop\Truata\groceries.csv"


#Creating RDD/reading CSV using textFile
Groceries_List = sc.textFile(path)


#Using flatmap to create new rdd for every word using comma
Groceries_List = Groceries_List.flatMap(lambda x: x.split(","))


#Using dintinct to find unique Groceries
Distinct_Groceries = Groceries_List.distinct()


#Reading all distinct Groceries.
Distinct_Groceries.collect()


#Writing data in ouput file
with open('out_part-1_task-1_2a.txt', 'w') as f:
    for line in Groceries_List.collect():
        f.write(line)
        f.write('\n')
