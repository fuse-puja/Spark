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
