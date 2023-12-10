# Imply Evaluation: Spark-SQL

## Initial Setup

```scala
file_location = "/Users/rohitraj/Downloads/nyctaxi"
df = spark.read.parquet(file_location)
temp_table_name = "yellow_tripdata"
df.createOrReplaceTempView(temp_table_name)
```
### 1.**Ranking and Percentiles:** Rank the top 10% of drivers based on their total earnings, considering both fare_amount and tip_amount.

>after analyzing the data dictionary for the dataset NYC Yellow Taxi a discrepancy in the question came up. There is no attribute which can be mapped to the drivers or cabs hence finding income aggregate at the level of drivers is not possible.
```sql
/*
Ranking and Percentiles: Rank the top 10% of drivers based on their total earnings, considering both fare_amount and tip_amount.
*/
select ROW_NUMBER() OVER(ORDER BY fare_amount+tip_amount DESC) as rank, `VendorID`,`tpep_pickup_datetime`,`tpep_dropoff_datetime`,`passenger_count`,`trip_distance`,`RatecodeID`,`store_and_fwd_flag`,`PULocationID`,`DOLocationID`,`payment_type`,`fare_amount`,`extra`,`mta_tax`,`tip_amount`,`tolls_amount`,`improvement_surcharge`,`total_amount`,`congestion_surcharge`,`Airport_fee` from yellow_tripdata where fare_amount+tip_amount >= (select percentile_approx(fare_amount,0.9,1000000000) from yellow_tripdata) order by fare_amount+tip_amount desc
```
### 2.**Payment Type Dynamics:** Determine if there is a significant difference in the average tip amount for credit card payments compared to other payment types.
```sql
/*
Payment Type Dynamics: Determine if there is a significant difference in the average tip amount for credit card payments compared to other payment types.
*/
select case when payment_type=1 then 'Credit Card' when payment_type=2 then 'Cash' else 'Other' end as payment_type ,avg(tip_amount) as average_tip_amount from yellow_tripdata where payment_type in (1,2) group by payment_type
```
### 3.**Dynamic Pricing Analysis:** Calculate the average fare amount per mile for each RatecodeID, taking into account the day of the week and time of day.
```sql 
/*
Dynamic Pricing Analysis: Calculate the average fare amount per mile for each RatecodeID, taking into account the day of the week and time of day.
*/
select CASE WHEN RatecodeID=1 then 'Standard rate' when RatecodeID=2 then 'JFK' when RatecodeID=3 then 'Newark'  when RatecodeID=4 then 'Nassau or Westchester' when RatecodeID=5 then 'Negotiated fare' when RatecodeID=6 then 'Group ride' else 'Other' end as RateCode,case when DAYOFWEEK(tpep_pickup_datetime) =1 then 'Sunday' when DAYOFWEEK(tpep_pickup_datetime) =2 then 'Monday' when DAYOFWEEK(tpep_pickup_datetime) =3 then 'Tuesday' when DAYOFWEEK(tpep_pickup_datetime) =4 then 'Wednesday' when DAYOFWEEK(tpep_pickup_datetime) =5 then 'Thursday' when DAYOFWEEK(tpep_pickup_datetime) =6 then 'Friday' when DAYOFWEEK(tpep_pickup_datetime) =7 then 'Saturday' ELSE NULL END as Day_of_Week , HOUR(tpep_pickup_datetime) as `Hour`,avg(fare_amount/trip_distance) as average_fare_per_mile from yellow_tripdata group by RatecodeID, DAYOFWEEK(tpep_pickup_datetime), HOUR(tpep_pickup_datetime) order by RatecodeID,DAYOFWEEK(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)
```
### 4.**Advanced Date and Time Analysis:** Identify the top 5 busiest hours of the week in terms of total trips.

```sql
/*
Advanced Date and Time Analysis: Identify the top 5 busiest hours of the week in terms of total trips.
*/
select case when DAYOFWEEK(tpep_pickup_datetime) =1 then 'Sunday' when DAYOFWEEK(tpep_pickup_datetime) =2 then 'Monday' when DAYOFWEEK(tpep_pickup_datetime) =3 then 'Tuesday' when DAYOFWEEK(tpep_pickup_datetime) =4 then 'Wednesday' when DAYOFWEEK(tpep_pickup_datetime) =5 then 'Thursday' when DAYOFWEEK(tpep_pickup_datetime) =6 then 'Friday' when DAYOFWEEK(tpep_pickup_datetime) =7 then 'Saturday' ELSE NULL END as Day_of_Week,hour(tpep_pickup_datetime) as hour_of_day,count(1) as number_of_trips from yellow_tripdata group by DAYOFWEEK(tpep_pickup_datetime), hour(tpep_pickup_datetime) order by count(1) Limit 5
```

### 5.**Geospatial Analysis:** Find the shortest path (in terms of distance) between two given locations (PULocationID and DOLocationID) using the available trip data.
```sql
/*
Geospatial Analysis: Find the shortest path (in terms of distance) between two given locations (PULocationID and DOLocationID) using the available trip data.
*/
select PULocationID,DOLocationID,min(trip_distance) as minimum_distance from yellow_tripdata group by PULocationID,DOLocationID order by PULocationID,DOLocationID
```