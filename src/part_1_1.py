#Importing Spark Module
from pyspark import SparkContext


#Creating Spark Context Object
sc = SparkContext.getOrCreate()

#Setting Path of file
path = "C:\Users\Mitul\OneDrive\Desktop\Truata\groceries.csv"


#Creating RDD/reading CSV using textFile
Groceries_List = sc.textFile(path)

#Reading first five records
Groceries_List.take(5)

#Writing data in ouput file
with open('out_part_1_task_1.txt', 'w') as f:
    for line in Groceries_List.take(5):
        f.write(line)
        f.write('\n')
