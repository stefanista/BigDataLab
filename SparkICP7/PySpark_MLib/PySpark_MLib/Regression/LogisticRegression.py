from pyspark.ml.classification import LogisticRegression

# Load training data
from pyspark.python.pyspark.shell import spark


from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("C:\\Users\\steph\\Downloads\\PySpark_MLib\\PySpark_MLib\\classification\\adultData")
data = data.withColumnRenamed("age", "label").select("label", col(" education-num").alias("education-num"), col(" hours-per-week").alias("hours-per-week"))
data = data.select(data.label.cast("double"), "education-num", "hours-per-week")

assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
training = assembler.transform(data)

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(training)

# Print the coefficients and intercept for logistic regression
print("Coefficients: " + str(lrModel.coefficientMatrix))
print("Intercept: " + str(lrModel.interceptVector))

# We can also use the multinomial family for binary classification
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

# Fit the model
mlrModel = mlr.fit(training)

# Print the coefficients and intercepts for logistic regression with multinomial family
print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
print("Multinomial intercepts: " + str(mlrModel.interceptVector))