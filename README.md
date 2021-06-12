# data-engineering-resources
A repository containing all the course demo and examples related to journey in the data engineering path. Feel free to fork

#### Spark With Scala Examples - Index
 - RatingsCounter
 - FriendsByAge
 - MinTemperature
 - MaxTemperature
 - WordCount
 - WordCountBetter
 - WordCountBetterSorted
 - PopularMovies
 - PopularMoviesNicer


#### Spark: The Definitive Guide - Index
 - Loading data from CSV
 - explain() over spark commands
 - Basic operations - in sql vs using dataframes
 - 


### Important Questions & Topics from Spark: The Definitive Guide
 - How does spark do joins ? Big-to-Big, Big-to-Small & Small-to-Small tables ?


### Udacity Data Engineering Nanodegree Notes
#### Introduction To Relational Databases
 - Why to use relational model ? Why not to use relational model ?
 - Why to use NoSQL model ? Why not to use NoSQL model ?
 - OLAP ? OLTP ? Difference ?
 - Normalization ? De-normalization ? When to use which ?
 - Fact and dimension tables
 - Star and snowflake schema
 - Eventual Consistency in NoSQL databases
 - CAP Theorem in NoSQL databases
 - Primary Key: Partition key and clustering columns(Ex- primary key((year), album_name))

#### Cassandra Notes
 - For any query that is using where on partition key and clustering cols, it must use them
   first, before applying where on any non-clustering column.
 - Clustering columns sort the data in desc order, when used multiple sort desc on the first
   column, then on the second column and so on.

### ETL and Data warehouses
 - Data warehouse - Business & Technical Perspectives
 - DWH Architectures
 - OLAP Cubes