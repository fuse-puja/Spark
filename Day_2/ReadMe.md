# PySpark Data Manipulation Examples

## Introduction

This repository provides a comprehensive guide to data manipulation and analysis using PySpark, a powerful tool for distributed data processing in Python. PySpark allows you to perform a wide range of operations on large datasets, making it an essential tool for big data analysis.
The dataset that has been used is `occupation.csv`

## Basic Structured Operations

### Selecting Specific Columns

- In PySpark, you can use the `select` operation to choose specific columns from a DataFrame.
- This operation is useful for focusing on specific attributes of your data and reducing the amount of information displayed.

### Filtering Rows based on Condition

- Filtering rows in PySpark is done using the `where` method or the `filter` method, allowing you to specify conditions.
- You can filter rows based on various criteria, such as age, gender, or any other column value.

### Counting and Grouping

- PySpark provides the `groupBy` operation to group rows based on a particular column.
- You can then use the `count` function to calculate the number of records in each group.
- This is handy for generating summary statistics from your data.

### Adding a New Column

- You can create new columns in a PySpark DataFrame using the `withColumn` method.
- The new column's values can be derived from existing columns or by applying conditional logic.
- This is useful for adding calculated or transformed data to your DataFrame.

### Creating DataFrames and Converting to Spark Types

- PySpark DataFrames are schema-based, allowing you to define the structure of your data.
- You can create DataFrames with specific schemas and convert data to Spark-compatible types.
- This ensures consistent data representation and facilitates data analysis.

### Adding and Renaming Columns

- PySpark enables you to add new columns to a DataFrame and rename existing ones.
- The `withColumn` method is used for adding columns, while `withColumnRenamed` is used for renaming columns.
- This provides flexibility in shaping your data for analysis.

### Filtering Rows and Sorting

- You can filter rows in a DataFrame to include only those that meet specific criteria.
- Sorting can be done using the `orderBy` or `sort` methods, allowing you to arrange data in ascending or descending order based on one or more columns.
- This is helpful for focusing on a subset of your data or arranging it for analysis.

### Repartitioning and Collecting Rows

- Repartitioning involves redistributing the data in a DataFrame into a specified number of partitions.
- The `coalesce` method can be used to reduce the number of partitions without shuffling data.
- Collecting rows allows you to retrieve data from partitions and work with it on the driver node.
- Understanding the number of partitions is crucial for optimizing data processing tasks.

## Additional Questions

This section addresses specific questions and tasks related to PySpark operations, such as filtering, calculating averages, combining DataFrames, and more. It demonstrates how to approach different data manipulation challenges using PySpark.


