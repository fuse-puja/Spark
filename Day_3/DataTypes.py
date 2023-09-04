from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, avg, when, coalesce, split, explode, create_map
from pyspark.sql.types import IntegerType

# Step 1: Initialize PySpark Session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Step 2: Load the Dataset
data_path = "../data/titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)

data_path = '../data/chipotle.csv'  # Replace with the actual path
chipotle_df = spark.read.csv(data_path, header=True, inferSchema=True)

data_path = '../data/kalimati_tarkari_dataset.csv'  # Replace with the actual path
kalimati_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Display schema of all the dataset
print("Titanic Schema:")
titanic_df.printSchema()

print("\nChipotle Schema:")
chipotle_df.printSchema()

print("\nKalimati Schema:")
kalimati_df.printSchema()

# Converting to Spark Types
# Convert the "Fare" column from double to integer
titanic_df = titanic_df.withColumn("fare", col("fare").cast(IntegerType()))

# Display schema
print("\nTitanic Schema after converting 'Fare' column to Integer:")
titanic_df.printSchema()

# Working with Booleans
# Add a new column "IsAdult" that indicates whether a passenger is an adult (age >= 18) or not.
titanic_df.withColumn("IsAdult", expr("Age >= 18")).show()

# Working with Numbers
# Calculate the average age of male and female passengers separately
average_age_by_sex = titanic_df.groupBy("Sex").agg(avg("Age").alias("AverageAge"))
average_age_by_sex.show()

# Working with Strings
# Load the "chipotle" dataset and find the item names containing the word "Chicken."
chipotle_df.filter(chipotle_df.item_name.contains("Chicken")).show()

# Regular Expressions
# Load the "chipotle" dataset and find the items with names that start with "Ch" followed by any character.
chipotle_df.filter(col("item_name").rlike(r'^Ch.')).show()

# Working with Nulls in Data
# Count the number of passengers with missing age information
missing_age_titanic = titanic_df.filter(col("Age").isNull()).count()
print("Number of passengers with missing age:", missing_age_titanic)

# Coalesce
# Utilizing the Chipotle dataset, use the coalesce function to combine the "item_name" and "choice_description" columns into a new column named "OrderDetails."
chipotle_df.select("*", coalesce(col("item_name"), col("choice_description")).alias("OrderDetails")).show(5)

# ifnull, nullIf, nvl, and nvl2
# Replace the null values in the "Age" column of the Titanic dataset with the average age.
average_value = titanic_df.select(avg("Age")).collect()[0][0]
titanic_nnull = titanic_df.fillna({"Age": average_value})
titanic_nnull.show()

# drop
# Remove the "Cabin" column from the Titanic dataset.
titanic_df = titanic_df.drop("Cabin")
titanic_df.show()

# fill
# Fill the null values in the "Age" column of the Titanic dataset with a default age of 30.
default = 30
titanic_df = titanic_df.fillna(default, subset=["Age"])
titanic_df.show()

# replace
# Replace the gender "male" with "M" and "female" with "F" in the "Sex" column of the Titanic dataset.
titanic_df = titanic_df.withColumn("Sex", when(col("Sex") == "male", "M").otherwise("F"))
titanic_df.show()

# Working with Complex Types: Structs
# Create a new DataFrame from the Kalimati Tarkari dataset, including a new column "PriceRange" that is a struct containing "Minimum" and "Maximum" prices for each commodity.

# Working with Complex Types: Arrays
# Create a new DataFrame from the Kalimati Tarkari dataset, including a new column "CommodityList" that is an array of all the commodities.
k_df = kalimati_df.select("*", split(col("Commodity"), "   ").alias("CommodityList"))
k_df.show(truncate=False)

# Working with Complex Types: explode
# Explode the "CommodityList" array column from the previous step to generate a new row for each commodity in the list.
exploded_df = k_df.select("SN", "Date", "Unit", "Minimum", "Maximum", "Average", explode("CommodityList").alias("Commodity"))
exploded_df.show(truncate=False)

# Working with Complex Types: Maps
# Create a new DataFrame from the Kalimati Tarkari dataset, including a new column "PriceMap" that is a map with "Commodity" as the key and "Average" price as the value.
kalimati_tarkari = kalimati_df.select("*", create_map(col("Commodity"), col("Average")).alias("PriceMap"))
kalimati_tarkari.show(truncate=False)

# Working with JSON
# Convert the "kalimati_df" DataFrame to JSON format and write it to a JSON file.
kalimati_json = kalimati_df.toJSON().collect()
filename = "Kalimati.json"

with open(filename, "w") as f:
    for json_row in kalimati_json:
        f.write(json_row + "\n")

print("Data written to", filename)
df = spark.read.json(spark.sparkContext.parallelize(kalimati_json))
df.show()
