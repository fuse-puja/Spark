from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, approx_count_distinct, first, last, min, max, sum, avg, struct
from pyspark.sql.window import Window

# Step 1: Initialize PySpark Session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Step 2: Load the Dataset
data_path = "../data/US_Crime_Rates_1960_2014.csv"  # Replace with the actual path
US_Crime_Rates_1960_2014_df = spark.read.csv(data_path, header=True, inferSchema=True)

data_path = "../data/titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)

US_Crime_Rates_1960_2014_df.printSchema()
US_Crime_Rates_1960_2014_df.show()
US_Crime_Rates_1960_2014_df.createOrReplaceTempView("crime_rates")

# Step 3: count
row_count = US_Crime_Rates_1960_2014_df.count()
print("Number of records =", row_count)

# Step 4: countDistinct
distinct_years = US_Crime_Rates_1960_2014_df.select(countDistinct("Year"))
print("No. of distinct years= ", distinct_years.collect()[0][0])

# Step 5: approx_count_distinct
total = US_Crime_Rates_1960_2014_df.select(approx_count_distinct("Total", 0.1))
print("Approximate distinct values in 'Total' column: ", total.collect()[0][0])

# Step 6: first and last
first_and_last_values = US_Crime_Rates_1960_2014_df.select(first("Year"), last("Year"))
print("First year =", first_and_last_values.collect()[0][0])
print("Last year =", first_and_last_values.collect()[0][1])

# Step 7: min and max
min_max = US_Crime_Rates_1960_2014_df.select(min("Population"), max("Population"))
print("Minimum population =", min_max.collect()[0][0])
print("Maximum population =", min_max.collect()[0][1])

# Step 8: sumDistinct
US_Crime_Rates_1960_2014_df.groupBy("Year").agg(sum_distinct("Property").alias("SumDistinctProperty")).show()

# Step 9: avg
avg_murder = US_Crime_Rates_1960_2014_df.select(avg("Murder"))
print("Average murder rate: ", avg_murder.collect()[0][0])

# Step 10: Aggregating to Complex Types
total_sum = US_Crime_Rates_1960_2014_df.groupBy("Year").agg(
    struct(
        sum(col("Violent")).alias("TotalViolent"),
        sum(col("Property")).alias("TotalProperty")
    ).alias("CrimeSums ")
)
total_sum.show()

# Step 11: Grouping
total_sum_crime = US_Crime_Rates_1960_2014_df.withColumn("AVG_Crime",
                                                        col('Violent') + col('Property') + col('Murder') + col(
                                                            'Forcible_Rape') +
                                                        col('Robbery') + col('Aggravated_assault') + col(
                                                            'Burglary') + col('Larceny_Theft')
                                                        + col('Vehicle_Theft'))

average_crime = total_sum_crime.agg(avg('AVG_Crime'))
print("Average of all crime:", average_crime.collect()[0][0])
total_sum_crime.select('Year', 'AVG_Crime').show()

# Step 12: Window Functions
window_spec = Window.partitionBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

US_Crime_Rates_1960_2014_df_window = US_Crime_Rates_1960_2014_df.withColumn(
    "CumulativePropertySum", sum(col("Property")).over(window_spec)
)
US_Crime_Rates_1960_2014_df_window.show()

# Step 13: Pivot
crimes = ["Violent", "Property", "Murder", "Forcible_Rape", "Robbery", "Aggravated_assault", "Burglary",
          "Larceny_Theft", "Vehicle_Theft"]

selected_df = US_Crime_Rates_1960_2014_df.select(crimes)
selected_df.show()

pivoted_df = selected_df.groupBy("Year").pivot(crimes).sum()
pivoted_df.show()
