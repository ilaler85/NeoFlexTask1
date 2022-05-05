from os import name, read
from sys import path
from typing import Counter
import zipfile
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.pandas.conversion import PandasConversionMixin
from pyspark.sql.types import StringType
PATH = "C:\\Users\\ivano\\Downloads\\Telegram Desktop"
count = 1

def openZip(pathZip):
    tmp = PATH +"\\"+ pathZip+'.zip'
    zipF = zipfile.ZipFile('{}'.format(tmp))
    zipF.printdir()
    zipF.extract("{}.xml".format(pathZip) ,path = PATH)
    zipF.close()

def DF (namefile):
    global count
    tmp = PATH+"\\"+namefile+'.xml'
    fileDF = spark.read.format("xml").load(tmp).option("rootTags", "triggers").option("rowTag","response")
    fileDF.write.parquet("data/test/key={}".format(count))
    count+=1

spark = SparkSession.builder.getOrCreate()
openZip('TR_029_20210115_1_1')
openZip('TR_029_20210118_1_1')
DF('TR_029_20210115_1_1')
DF('TR_029_20210118_1_1')
mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test")
print(mergedDF)
