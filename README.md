from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("Python").master("local[2]").getOrCreate()
data=r"C:\bigdata\drivers\books.xml"
df=spark.read.format("xml").option("rowTag","book").load(data)
#df.show()
res=df.groupBy('genre').agg(count('price').alias('count'),avg('price').alias('total_price')).orderBy(desc('total_price'))
res.show()

data1=r"C:\bigdata\drivers\resume_w_xsl.xml"
df1=spark.read.format("xml").option("inferSchema","true").load(data1)
df1.show(truncate=False)
df1.printSchema()
##since we are not seeing any datastructure of xml then lets inspect with python program read the file

with open(data1,'r') as f:
    xmldata=f.read()
    print(xmldata)
print("##now read the xml file with rowTag")
df2=spark.read.format("xml").option("rowTag","project").load(data1)
df2=df2.withColumnRenamed("_end","end").withColumnRenamed("_start","start")
df2.show(truncate=False)
df2.printSchema()

