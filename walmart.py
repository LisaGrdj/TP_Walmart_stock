from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__=="__main__":

    spark = SparkSession \
    .builder \
    .appName("Walmart stock") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    # lecture du ficher (q2)
    df = spark.read.csv("walmart_stock.csv")
    # les colonnes (q3)
    df.columns
    # schéma  (q4)
    df.printSchema()
    # new dataframe (q5)
    newdf=df.withColumn('Hv_Ratio', col('High')/col('Volume'))
    # mean of Close (q7)
    df.agg({'Close':'mean'}).show()
    # min et max of Volume (q8)
    df.select(min('Volume')).show()
    df.select(max('Volume')).show()
    # close < 60 (q9)
    df.filter(df.Close<60).show()
    # max oh High (q11)
    df.select(max('High')).show()
