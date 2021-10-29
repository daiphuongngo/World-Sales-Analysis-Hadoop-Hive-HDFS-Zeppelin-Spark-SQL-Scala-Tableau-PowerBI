# World-Sales-Analysis-Zeppelin-Spark-Scala-SQL

## Overview:

## Dataset:

worldsales.csv

## Language and Tools:

- Hadoop

- HDFS

- Hive

- Zeppelin

- Spark

- Scala

- SQL

## Table of Content:

### 1. Load data

#### Load data into HDFS

Upload the file worldsales.csv to HDFS’s tmp folder

![image](https://user-images.githubusercontent.com/70437668/139508083-dc68c7a1-1308-40d1-ab5b-9a048d2c7765.png)

#### Load data into Zeppelin

```
// Create a worldsales DataFrame from CSV file
%spark2
val worldsales = (spark.read 
.option("header", "true") // Use first line as header
.option("inferSchema", "true") // Infer schema
.csv("/tmp/worldsales.csv"))
```

### 2.	Show the newly created dataframe 

```
%spark2
worldsales.select(“Id”, “Region”, “Country”, “Item_Type”, “Sales_Channel”, “Order_Priority”, “Order_Date”, “Order_ID”, “Ship_Date”, “Units_Sold”, “Unit_Price”, “Unit_Cost”, “Total_Revenue”, “Total_Cost”, “Total Profit”).show()
```

### 3. Print the dataframe schema  
```
%spark2
// Print the Schema in a tree format
worldsales.printSchema()
```

### 4. Filter the dataframe to show units sold > 8000 and unit cost > 500

#### Method 1:
```
%spark2
// Create a Dataset containing worldsales with units sold and unit price using “filter”
val worldsales_dataset = worldsales.select((“Id”, “Region”, “Country”, “Item_Type”, “Sales_Channel”, “Order_Priority”, “Order_Date”, “Order_ID”, “Ship_Date”, “Units_Sold”, “Unit_Price”, “Unit_Cost”, “Total_Revenue”, “Total_Cost”, “Total Profit”)
					.filter($”Units_Sold” > 8000)
					.filter($”Unit_Cost” > 500)
worldsales_dataset.show()
```

#### Method 2:
```
worldsales.filter("Units_Sold" > 8000 && "Unit_Cost" > 500).show()
```

### 5.	Show the dataframe in group by “Region” and count
```
%spark2
worldsales.groupBy(“region”).count().show()
```

### 6.	Show the dataframe in group by “Region” and count
```
%spark2
worldsales.groupBy(“region”).count().show()
```

### 7.	Save the new subset dataframe as a CSV file into HDFS
```
%spark2
worldsales_results.coalesce(1).write.format(“csv”).option(“header”, “true”).save(“/tmp/worldsales_results.csv”)
```

### 8.	Create two views using the “createOrReplaceTempView” command

#### 8.a.	View on “Salesview” from the first dataframe
```
%spark2
worldsales.createOrReplaceTempView(“Salesview”)
```

#### 8.b.	View on “Regionview” from the second dataframe 
```
%spark2
Worldsales_results.createOrReplaceTempView(“Regionview”)
```

### 9.	Using SQL select all from “Regionview” view and show in a line graph 
```
%spark2.sql
select * from Regionview
```

### 10.	Using SQL select from the “Salesview” view – the region and sum of units sold and group by region  and display in a data grid view 
```
%spark2.sql
select region, sum(Units_Sold) as Sum_Units_Sold
from Salesview
group by region
```

### 11.	Using SQL select from the “Salesview” view – the region and sum of total_profit and group by region and display in a Bar chart 
```
%spark2.sql
select region, sum(Total_Profit)
from Salesview
group by region
```

### 12.	Using SQL select from the “Salesview” view – show the total profit as profit, the total revenue as revenue and the total cost as cost from “Salesview” group by region – The client wants to see this data in a line graph so as to see the correlation between cost ,revenue ,profit between regions.
```
%spark2.sql
select region, Sum(Total_Profit) as Profit, Sum(total_revenue) as Revenue, Sum(total_cost) as Cost
from Salesview
group by region
```

### 13.	The customer is in the process of opening up a new store an they are looking at the best location to do so, they need to see the avg profit in each region as a percentage (pie chart) compared to other regions, please use both views created to demonstrate answer also point out the region where it is most profitable. 
```
%spark2.sql
select a.Region, avg(Total_Profit) 
from Salesview b , Regionview a
where a.Region = b.Region
group by a.Region
```

--> Europe's Average Profit at 27% is the highest 





