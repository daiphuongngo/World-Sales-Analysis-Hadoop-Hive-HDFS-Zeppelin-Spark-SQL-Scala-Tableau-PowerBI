/*  World-Sales-Analysis-Hadoop-Hive-Zeppelin-Spark-SQL-Scala-Tableau

## Overview:
The World Sales here is a small dataset used to demonstrate skills in both Big Data and Visualization platforms, languages and tools. The target of this project is to determine the most profitable Region and Country by different factors and methods.

## Platforms, Languages and Tools:
- Hadoop
- HDFS
- Hive
- Zeppelin
- Spark
- Scala
- SQL

## Table of Content:
1. Load data
Load data into HDFS */

--2. Create external table in Hive for analysis in Zeppelin
-- Hive
CREATE EXTERNAL TABLE IF NOT EXISTS worldsales (Id INT, Region STRING, Country STRING, Item_Type STRING, Sales_Channel STRING, Order_Priority STRING, Order_Date DATETIME, Order_ID INT, 
Ship_Date DATETIME, Units_Sold INT, Unit_Price INT, Unit_Cost INT, Total_Revenue DOUBLE, Total_Cost DOUBLE, Total_Profit DOUBLE)
COMMENT 'Data of the World Sales'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/worldsales'

-- Zeppelin
-- Spark
--// Create a worldsales DataFrame from CSV file
%spark2
val worldsales = (spark.read 
.option("header", "true") --// Use first line as header
.option("inferSchema", "true") --// Infer schema
.csv("/tmp/worldsales.csv"))

--3. Show the newly created dataframe 
%spark2
worldsales.select("Id", "Region", "Country", "Item_Type", "Sales_Channel", "Order_Priority", "Order_Date", "Order_ID", "Ship_Date", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", 
                  "Total_Cost", "Total Profit").show()

--4. Print the dataframe schema in a tree format
%spark2
worldsales.printSchema()

--5. Filter the dataframe to show units sold > 8000 and unit cost > 500
-- Method 1: // Create a Dataset containing worldsales with units sold and unit price using “filter”
%spark2
val worldsales_dataset = worldsales.select(("Id", "Region", "Country", "Item_Type", "Sales_Channel", "Order_Priority", "Order_Date", "Order_ID", "Ship_Date", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", "Total_Cost", "Total Profit")
					.filter($"Units_Sold" > 8000)
					.filter($"Unit_Cost" > 500)
worldsales_dataset.show()

--Method 2:
worldsales.filter("Units_Sold" > 8000 && "Unit_Cost" > 500).show()

--6.	Show the dataframe in group by “Region” and count
%spark2
worldsales.groupBy("region").count().show()

--7.	Create a separate dataframe with the group by results
%spark2
val worldsales_results = worldsales.groupBy(“region”).count()
worldsales_results.show()

--8.	Save the new subset dataframe as a CSV file into HDFS
%spark2
worldsales_results.coalesce(1).write.format("csv").option("header", "true").save("/tmp/worldsales_results.csv")


--9.	Create two views using the “createOrReplaceTempView” command
-- 9.a.	View on “Salesview” from the first dataframe
%spark2
worldsales.createOrReplaceTempView("Salesview")

--9.b.	View on “Regionview” from the second dataframe 
%spark2
Worldsales_results.createOrReplaceTempView("Regionview")

-- SQL
--10.	Using SQL select all from “Regionview” view and show in a line graph 
%spark2.sql
SELECT * FROM Regionview

--11.	Using SQL select from the “Salesview” view – the region and sum of units sold and group by region and display in a data grid view 
%spark2.sql
SELECT region, SUM(Units_Sold) AS Sum_Units_Sold
FROM Salesview
GROUP BY region

--12.	Using SQL select from the “Salesview” view – the region and sum of total_profit and group by region and display in a Bar chart 
%spark2.sql
SELECT region, SUM(Total_Profit)
FROM Salesview
GROUP BY region

--13.	From the “Salesview” view, show the Total Profit as Profit, the Total Revenue as Revenue and the Total Cost as Cost from “Salesview” group by Region 
-- The client wants to see this data in a Line chart in order to see the correlation between Cost, Revenue, Profit between Regions.
%spark2.sql
SELECT region, SUM(Total_Profit) AS Profit, SUM(total_revenue) AS Revenue, SUM(total_cost) AS Cost
FROM Salesview
GROUP BY region

--14.	The customer is planning to open up a new store and searching for the best location for it, they need to see the Average Profit in each Region as a percentage (Pie chart) compared to other Regions
--Now I will use both views created to plot the Pie chart and also point out the region where it is most profitable. 
%spark2.sql
SELECT a.Region, AVG(Total_Profit) 
FROM Salesview b , Regionview a
WHERE a.Region = b.Region
GROUP BY a.Region