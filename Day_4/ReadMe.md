# Aggregations README

## Introduction

This README file provides comprehensive instructions and examples for performing various aggregations on a dataset using PySpark and SQL. The dataset used in these examples contains crime rate data for the United States spanning from 1960 to 2014.

## Table of Contents

1. [Initialize PySpark Session](#initialize-pyspark-session)
2. [Load the Dataset](#load-the-dataset)
3. [Count](#count)
4. [Count Distinct](#count-distinct)
5. [Approximate Count Distinct](#approximate-count-distinct)
6. [First and Last](#first-and-last)
7. [Min and Max](#min-and-max)
8. [Sum Distinct](#sum-distinct)
9. [Average](#average)
10. [Aggregating to Complex Types](#aggregating-to-complex-types)
11. [Grouping](#grouping)
12. [Window Functions](#window-functions)
13. [Pivot](#pivot)

---
# Joins 

## Introduction

This README file provides comprehensive instructions and examples for performing various types of joins between DataFrames using PySpark. We will work with two DataFrames: `employees_df` and `departments_df`, which contain employee and department information, respectively.

## Table of Contents

1. [Join Expressions](#join-expressions)
2. [Inner Joins](#inner-joins)
3. [Outer Joins](#outer-joins)
4. [Left Outer Joins](#left-outer-joins)
5. [Right Outer Joins](#right-outer-joins)
6. [Left Semi Joins](#left-semi-joins)
7. [Left Anti Joins](#left-anti-joins)
8. [Cross (Cartesian) Joins](#cross-cartesian-joins)
   
---


# Initialize PySpark Session

Initialize a PySpark session to set up the environment for data analysis and aggregation. PySpark is a powerful tool for processing large datasets using the Apache Spark framework.

# Load the Dataset

Load the crime rate dataset into a PySpark DataFrame for further analysis. Loading data is the first step in any data analysis project.

# Count

Learn how to count the total number of records in a DataFrame using both PySpark and SQL. This fundamental operation helps you understand the size of your dataset.

# Count Distinct

Find the count of distinct values in specific columns of the DataFrame. Useful for understanding unique data points in categorical columns.

# Approximate Count Distinct

Estimate the count of distinct values with approximate calculations for memory efficiency. Ideal for large datasets when an exact count is resource-intensive.

# First and Last

Discover how to find the first and last values in a DataFrame column. Useful for identifying temporal trends or extreme values.

# Min and Max

Calculate the minimum and maximum values within a DataFrame column. Useful for understanding the range and extremes of a dataset.

# Sum Distinct

Find the sum of distinct values in a particular column of the DataFrame. Helpful for summarizing unique contributions to a total.

# Average

Calculate the average (mean) of values in a DataFrame column. Provides insights into central tendencies of data.

# Aggregating to Complex Types

Explore methods to aggregate data into complex types, such as structs, within a DataFrame. Useful for hierarchical data structures.

# Grouping

Understand how to group and aggregate data by one or more columns in the DataFrame. Essential for summarizing data by categories.

# Window Functions

Use window functions to perform calculations over a specified range of rows within the DataFrame. Useful for running calculations across sliding windows of data.

# Pivot

Learn how to pivot data to restructure it based on column values, facilitating analysis. Helpful for transforming data for reporting or visualization purposes.

---

# Joins

# Join Expressions
- Join expressions allow you to combine DataFrames based on common columns.
- Common expressions include inner, outer, left outer, right outer, semi, and anti joins.
- Join expressions are essential for merging data from multiple sources or tables.

# Inner Joins
- Inner joins return rows where a condition is met in both DataFrames.
- Useful for extracting data that exists in both tables.
- Example: Retrieving employees in the "Engineering" department.

# Outer Joins
- Outer joins combine data from both DataFrames, filling missing values with specified defaults.
- Common types include left outer, right outer, and full outer joins.
- Useful for preserving all data, even if it doesn't have a corresponding match in the other DataFrame.

# Left Outer Joins
- Left outer joins return all rows from the left DataFrame and matching rows from the right DataFrame.
- Suitable for scenarios where you want to keep all records from one DataFrame and only matching records from the other.

# Right Outer Joins
- Right outer joins are similar to left outer joins but prioritize keeping all records from the right DataFrame.
- Useful when you want to retain all department records, including those without employees.

# Left Semi Joins
- Left semi joins return rows from the left DataFrame where a condition is met in the right DataFrame.
- Valuable for extracting data from the left DataFrame based on a condition in the right DataFrame.

# Left Anti Joins
- Left anti joins return rows from the left DataFrame where a condition is NOT met in the right DataFrame.
- Helpful for identifying records in the left DataFrame that have no corresponding match in the right DataFrame.

# Cross (Cartesian) Joins
- Cross joins generate a DataFrame containing all possible combinations of rows from both DataFrames.
- Resulting DataFrame can be large, so use with caution.
- Useful when you need to explore all combinations of data.

These join operations are fundamental for data integration and analysis, helping you extract valuable insights from diverse datasets. Choose the appropriate join type based on your specific data manipulation needs.

