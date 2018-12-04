from pyspark.ml.clustering import KMeans


# Loads data.
from pyspark.python.pyspark.shell import spark

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

#only numbers!

data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("C:\\Users\\steph\\Downloads\\PySpark_MLib\\PySpark_MLib\\classification\\adultData")
data = data.withColumnRenamed("age", "label").select("label", col(" education-num").alias("education-num"), col(" hours-per-week").alias("hours-per-week"))
data = data.select(data.label.cast("double"), "education-num", "hours-per-week")

assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
dataset = assembler.transform(data)

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)