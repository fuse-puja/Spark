# Getting started with Pyspark

## Initialize PySpark Session

Initialize a PySpark session using `SparkSession` to create the environment for data analysis. This step is essential for working with Spark DataFrames.

## Load the Dataset

Load the dataset into a Spark DataFrame using `spark.read.csv()` to prepare it for analysis. In this case, the Chipotle dataset is loaded for exploration.

## Get an Overview of the DataFrame

Explore the dataset's structure and content. Use `df.printSchema()` to print the schema and understand the data types of each column. Employ `df.show()` to display the first few rows of data and get a glimpse of the dataset.

## Calculate Basic Statistics

Compute basic statistics for numeric columns within the dataset using `df.describe()`. This PySpark function provides insights into central tendencies, variability, and distribution of numerical data.

## Number of Observations

Determine the total number of observations or rows present in the dataset using `df.count()`. This PySpark function gives an understanding of the dataset's size.

## Number of Columns

Find out the total number of columns or features in the dataset using `len(df.columns)`. This PySpark function helps identify the dataset's dimensionality.

## Column Names

List and display the names of all columns in the dataset with `df.columns`. Understanding column names is crucial for referencing and analyzing specific attributes of the data.

These steps are fundamental for initial data exploration and preparation before diving into more in-depth analysis and processing tasks using PySpark functions.
