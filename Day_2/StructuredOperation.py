from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Step 1: Initialize PySpark Session
spark = SparkSession.builder.appName("day2").getOrCreate()

# Step 2: Load the Dataset
data_path = "../data/occupation.csv"  # Replace with the actual path
occupation = spark.read.csv(data_path, header=True, inferSchema=True)

# Display the schema
occupation.printSchema()

# Problem 1: Selecting Specific Columns
selected_columns = occupation.select("user_id", "age", "occupation")
selected_columns.show()

# Problem 2: Filtering Rows based on Condition
occupation.select("*").where("age > 30").show()

# Problem 3: Counting and Grouping
occupation.groupBy("occupation").count().show()

# Problem 4: Adding a New Column
occupation_with_age_group = occupation.withColumn("age_group",
    when((occupation.age >= 18) & (occupation.age <= 25), lit("18-25"))
    .when((occupation.age >= 26) & (occupation.age <= 35), lit("26-35"))
    .when((occupation.age >= 36) & (occupation.age <= 50), lit("36-50"))
    .otherwise(lit("51+"))
)
occupation_with_age_group.show()

# Problem 5: Creating DataFrames and Converting to Spark Types
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", DoubleType(), True)
])

data = [("James", None, "Smith", 36636, "M", 3000.0),
        ("Michael", "Rose", None, 40288, "M", 4000.0),
        ("Robert", None, "Williams", 42114, "M", 4000.0),
        ("Maria", "Anne", "Jones", 39192, "F", 4000.0),
        ("Jen", "Mary", "Brown", None, "F", -1.0)]

myDf = spark.createDataFrame(data, schema)

myDf.printSchema()
myDf.show()

# Problem 6: Adding and Renaming Columns
new_occupation = occupation.withColumn("gender", lit("Unknown")).withColumnRenamed("age", "Years")
new_occupation.show()

# Problem 7: Filtering Rows and Sorting
filtered_occupation = occupation.filter(col("age") > 30)
sorted_occupation = filtered_occupation.orderBy(col("age").desc())
sorted_occupation.show()

# Problem 8: Repartitioning and Collecting Rows
myDf = myDf.coalesce(2)
rows = myDf.collect()
for row in rows:
    print(row)
print("Number of partitions a:", myDf.rdd.getNumPartitions())
