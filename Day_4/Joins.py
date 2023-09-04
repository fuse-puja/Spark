from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, coalesce, lit

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Define the schema using StructType and StructField for employees
custom_employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])

# Define the data for employees
data = [
    (1, "Pallavi mam", 101),
    (2, "Bob", 102),
    (3, "Cathy", 101),
    (4, "David", 103),
    (5, "Amrit Sir", 104),
    (6, "Alice", None),
    (7, "Eva", None),
    (8, "Frank", 110),
    (9, "Grace", 109),
    (10, "Henry", None)
]

# Create a DataFrame for employees with the custom schema
employee_df = spark.createDataFrame(data, schema=custom_employee_schema)

# Define the schema using StructType and StructField for departments
custom_department_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department_name", StringType(), True)
])

# Define the data for departments
department_data = [
    (101, "HR"),
    (102, "Engineering"),
    (103, "Finance"),
    (104, "Marketing"),
    (105, "Operations"),
    (106, None),
    (107, "Operations"),
    (108, "Production"),
    (None, "Finance"),
    (110, "Research and Development")
]

# Create a DataFrame for departments with the custom schema
department_df = spark.createDataFrame(department_data, custom_department_schema)

# Register the DataFrames as temporary SQL tables
employee_df.createOrReplaceTempView("employees")
department_df.createOrReplaceTempView("departments")

# Perform an inner join between employees_df and departments_df DataFrames
combined_df = employee_df.join(department_df, on="department_id", how="inner")

# Select and display specific columns
combined_df.select(col("department_id"), col("employee_id"), col("employee_name"), col("department_name")).show()

# Perform an inner join operation using SQL
query = """
    SELECT e.department_id, e.employee_id, e.employee_name, d.department_name
    FROM employees e
    INNER JOIN departments d ON e.department_id = d.department_id
"""

# Execute the SQL query and display the result
combined_df = spark.sql(query).show()

# Left Outer Join
left_outer_df = employee_df.join(department_df, on="department_id", how="left_outer")
left_outer_df.select(col("department_id"), col("employee_id"), col("employee_name"), col("department_name")).show()

# Right Outer Join
right_outer_df = employee_df.join(department_df, on="department_id", how="right_outer")
right_outer_df.select(col("department_id"), col("employee_id"), col("employee_name"), col("department_name")).show()

# Left Semi Join
left_semi_df = employee_df.join(department_df, on="department_id", how="left_semi")
left_semi_df.select(col("employee_name")).show()

# Left Anti Join
left_anti_df = employee_df.join(department_df, on="department_id", how="left_anti")
left_anti_df.select(col("employee_name")).show()

# Cross Join
cross_join_df = employee_df.crossJoin(department_df)
cross_join_df.show()
