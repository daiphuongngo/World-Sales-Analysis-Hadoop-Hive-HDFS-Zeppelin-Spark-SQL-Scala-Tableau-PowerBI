# World-Sales-Analysis-Hadoop-HDFS-Zeppelin-Spark-SQL-Scala-Tableau

## Overview:

The World Sales here is a small dataset used to demonstrate skills in both Big Data and Visualization platforms, languages and tools. The target of this project is to determine the most profitable Region and Country by different factors and methods.

## Platforms, Languages and Tools:

- Hadoop

- HDFS

- Zeppelin

- Spark

- Scala

- SQL

## Dataset:

worldsales.csv

## Key variables:

| Id | Region | Country | Item_Type | Sales_Channel | Order_Priority | Order_Date | Order_ID | Ship_Date | Units_Sold | Unit_Price | Unit_Cost | Total_Revenue | Total_Cost | Total_Profit | 
|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|

## Conclusion:

Europe is the most profitable region. Belarus is the most profitable country worldwide.

## Table of Content:

### 1. Load data

#### Load data into HDFS

Upload the file worldsales.csv to HDFS’s tmp folder

![Screenshot 2021-06-23 214558](https://user-images.githubusercontent.com/70437668/139509148-a72afb57-ab13-4537-98bf-bcbf8550fbe6.png)

### 2. Create external table in Hive for analysis in Zeppelin
-- Hive
```
CREATE EXTERNAL TABLE IF NOT EXISTS worldsales (Id INT, Region STRING, Country STRING, Item_Type STRING, Sales_Channel STRING, Order_Priority STRING, Order_Date DATETIME, Order_ID INT, 
Ship_Date DATETIME, Units_Sold INT, Unit_Price INT, Unit_Cost INT, Total_Revenue DOUBLE, Total_Cost DOUBLE, Total_Profit DOUBLE)
COMMENT 'Data of the World Sales'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/worldsales'
```

-- Zeppelin
-- Spark

#### Load data into Zeppelin

```
// Create a worldsales DataFrame from CSV file
%spark2
val worldsales = (spark.read 
.option("header", "true") --// Use first line as header
.option("inferSchema", "true") --// Infer schema
.csv("/tmp/worldsales.csv"))
```

![Screenshot 2021-06-23 214558](https://user-images.githubusercontent.com/70437668/139515167-f08f5659-755f-4513-9a6c-5de3d52f753f.png)

### 3.	Show the newly created dataframe 

```
%spark2
worldsales.select("Id", "Region", "Country", "Item_Type", "Sales_Channel", "Order_Priority", "Order_Date", "Order_ID", "Ship_Date", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", "Total_Cost", "Total Profit").show()
```

![Screenshot 2021-06-23 220234](https://user-images.githubusercontent.com/70437668/139509159-ef5ddbc4-ac14-40a1-8c47-ccaf69de97cf.png)

### 4. Print the dataframe schema  
```
%spark2
// Print the Schema in a tree format
worldsales.printSchema()
```

![Screenshot 2021-06-23 220419](https://user-images.githubusercontent.com/70437668/139509177-65606a0a-ea5e-4744-bca0-7305d455637e.png)

### 5. Filter the dataframe to show units sold > 8000 and unit cost > 500

#### Method 1:
```
%spark2
// Create a Dataset containing worldsales with units sold and unit price using “filter”
val worldsales_dataset = worldsales.select(("Id", "Region", "Country", "Item_Type", "Sales_Channel", "Order_Priority", "Order_Date", "Order_ID", "Ship_Date", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", "Total_Cost", "Total Profit")
					.filter($"Units_Sold" > 8000)
					.filter($"Unit_Cost" > 500)
worldsales_dataset.show()
```

#### Method 2:
```
worldsales.filter("Units_Sold" > 8000 && "Unit_Cost" > 500).show()
```

![Screenshot 2021-06-23 221000](https://user-images.githubusercontent.com/70437668/139509184-81b9419d-24f9-4c01-a3c6-fe5505de97e0.png)

![Filter](https://user-images.githubusercontent.com/70437668/139518775-b0bfec6a-5f44-4671-b51a-2d5cceeda13f.jpg)

This led to only 2 Sub-Saharan countries: Senegal and Swaziland after the filteration. Senegal's Units Sold was 8,989 and Unit Cost was 502.54 while Swaziland's Units Sold was 9,915 and Unit Cost was 524.96. Both has the same Sales Channel as Offline.

### 6.	Show the dataframe in group by “Region” and count
```
%spark2
worldsales.groupBy("region").count().show()
```

![Q5 ](https://user-images.githubusercontent.com/70437668/139508998-7629e595-4100-4179-8564-fb2307e26dce.png)

![Count by Region](https://user-images.githubusercontent.com/70437668/139518778-ac9ba5d1-78c1-4c9e-8155-ff0d17553026.jpg)

### 7.	Create a separate dataframe with the group by results
```
%spark2
val worldsales_results = worldsales.groupBy("region").count()
worldsales_results.show()
```
![Q6](https://user-images.githubusercontent.com/70437668/139517566-ad7106c8-94f3-4656-bb50-66219e825c50.png)

There were the most activities in Sub-Saharan Africa and Europe. Meanwhile, North America and Australia and Oceania wasn't active in trading with Sub-Saharan Africa.

### 8.	Save the new subset dataframe as a CSV file into HDFS
```
%spark2
worldsales_results.coalesce(1).write.format(“csv”).option(“header”, “true”).save(“/tmp/worldsales_results.csv”)
```

![Q7 results](https://user-images.githubusercontent.com/70437668/139509021-3041b3e1-2a11-40fe-a45b-abbb61765275.png)

### 9.	Create two views using the “createOrReplaceTempView” command

#### 9.a.	View on “Salesview” from the first dataframe
```
%spark2
worldsales.createOrReplaceTempView(“Salesview”)
```

![Q8 1](https://user-images.githubusercontent.com/70437668/139509034-16f1823f-ccdb-4621-89bd-b3ba4c6b670d.png)

#### 9.b.	View on “Regionview” from the second dataframe 
```
%spark2
Worldsales_results.createOrReplaceTempView(“Regionview”)
```

![Q8 2](https://user-images.githubusercontent.com/70437668/139509039-11506d7e-3399-4a63-9173-4b2b3c6e9870.png)

### -- SQL
### 10.	Using SQL select all from “Regionview” view and show in a line graph 
```
%spark2.sql
SELECT * FROM Regionview
```

![Q9](https://user-images.githubusercontent.com/70437668/139509047-e583dd08-7f36-4922-a06f-b80e80ad643a.png)

![Region Line](https://user-images.githubusercontent.com/70437668/139518787-ddf51553-d2ee-4b53-83aa-8c2739804e62.jpg)

The Line chart illustrates the dynamic Sales & Trading activities between Europe and Sub-Saharan Africa. But there was no energetic performance between North America, Australia & Oceania and Sub-Sharan Africa.

### 11.	Using SQL select from the “Salesview” view – the region and sum of units sold and group by region and display in a data grid view 
```
%spark2.sql
SELECT region, SUM(Units_Sold) AS Sum_Units_Sold
FROM Salesview
GROUP BY region
```

![Q10](https://user-images.githubusercontent.com/70437668/139509057-dad4d738-e4d3-4a9e-ae11-a5bca4cf4ac8.png)

![Region vs Sum Units Sold](https://user-images.githubusercontent.com/70437668/139518793-ecd9e204-17dd-491e-ab75-12a4dccb7949.jpg)

There was a positive correlation of Sum Units Sold between Europe and Sub-Saharan Africa. These two regions and continents had the highest Sum Units Sold. In contrast, North America, Australia & Oceania had the lowest Sum Units Sold. Other regions and continents played moderately around the average Sum Units Sold.

### 12.	Using SQL select from the “Salesview” view – the region and sum of total_profit and group by region and display in a Bar chart 
```
%spark2.sql
SELECT region, SUM(Total_Profit)
FROM Salesview
GROUP BY region
```

![Q11](https://user-images.githubusercontent.com/70437668/139509063-26305d3c-b2cf-4dd5-8e89-7a8e6b3ffa65.png)

Europe and Sub-Saharan Africa certainly dominated Sum of Total Profit, ranking 2nd and 1st, respectively. North America, Australia & Oceania in the other hand gained the lowest Sum of Total Profit.

### 13.	From the “Salesview” view, show the Total Profit as Profit, the Total Revenue as Revenue and the Total Cost as Cost from “Salesview” group by Region – The client wants to see this data in a Line chart in order to see the correlation between Cost, Revenue, Profit between Regions.
```
%spark2.sql
SELECT region, SUM(Total_Profit) AS Profit, SUM(total_revenue) AS Revenue, SUM(total_cost) AS Cost
FROM Salesview
GROUP BY region
```

![Q12 have to drag ](https://user-images.githubusercontent.com/70437668/139509085-149fe26e-79df-4b6b-b6e8-2af0d3a9b983.png)

![Q12](https://user-images.githubusercontent.com/70437668/139509090-ac994010-86a9-4954-b104-1680483bf250.png)

![Cost, Revenue, Profit between Regions](https://user-images.githubusercontent.com/70437668/139518802-4a4e7f3a-f2a0-4072-9b97-860288748176.jpg)

The correlations between these fields between Regions were the same. Europe and Sub-Saharan Africa gained the highest figures in all 3 fields while North America, Australia & Oceania's fields were significantly low.

![Sum Cost between Regions on Map](https://user-images.githubusercontent.com/70437668/139518811-248cb430-3836-465d-8fe6-18b70dc26522.jpg)

![Sum Profit between Regions on Map](https://user-images.githubusercontent.com/70437668/139518817-4f978d7e-8d10-4299-9aff-a17fdfe7e76e.jpg)

![Sum Revenue between Regions on Map](https://user-images.githubusercontent.com/70437668/139518821-04aaa118-6919-4d9e-8a78-ada56d36dd30.jpg)

![Average Profit by Region on Map](https://user-images.githubusercontent.com/70437668/139518826-f9d1a798-4f11-4a87-9de6-65e9206ebbed.jpg)

### 14.	The customer is planning to open up a new store and searching for the best location for it, they need to see the Average Profit in each Region as a percentage (Pie chart) compared to other Regions

Now I will use both views created to plot the Pie chart and also point out the region where it is most profitable. 

```
%spark2.sql
SELECT a.Region, AVG(Total_Profit) 
FROM Salesview b , Regionview a
WHERE a.Region = b.Region
GROUP BY a.Region
```

![Q13 - Orren's solution on pie chart](https://user-images.githubusercontent.com/70437668/139509118-49ccd97a-31a5-40bf-9f35-bd668d27e906.png)

![Average Profit by Region](https://user-images.githubusercontent.com/70437668/139518850-10dcff21-9f60-4420-8358-917e71d32f1b.jpg)

The Pie chart demonstrates that Europe and Sub-Saharan Africa took half of the worldwide Total Profit. Europe's Average Profit at 27% is the highest among all continents. Thefore, it is proven that Europe would be the most profitable Region. 

More specifically, Belarus is the most profitable country by Average Profit.

### Dashboard - Sales Performance by Region

![Dashboard - Sales Performance by Region](https://user-images.githubusercontent.com/70437668/139518852-d25463ef-7bd2-49f6-b104-d99e0405d1b4.jpg)

### Dashboard - Maps of Cost, Revenue, Profit

![Dashboard - Maps of Cost, Revenue, Profit](https://user-images.githubusercontent.com/70437668/139518855-e0d6c73b-2163-468a-9af1-e238f9abb1f5.jpg)
