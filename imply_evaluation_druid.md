# Imply Take Home Assignment: Druid

### 1.**Ranking and Percentiles:** Rank the top 10% of drivers based on their total earnings, considering both fare_amount and tip_amount.


### 2.**Payment Type Dynamics:** Determine if there is a significant difference in the average tip amount for credit card payments compared to other payment types.
```sql
SELECT
  CASE WHEN payment_type = 1 THEN 'Credit Card' WHEN payment_type = 2 THEN 'Cash' ELSE 'Other' END AS payment_type,
  AVG(tip_amount) AS average_tip_amount
FROM nyctaxi
WHERE payment_type IN (1, 2)
GROUP BY payment_type
```


### 3.**Dynamic Pricing Analysis:** Calculate the average fare amount per mile for each RatecodeID, taking into account the day of the week and time of day.
```sql
SELECT
  CASE WHEN RatecodeID = 1 THEN 'Standard rate' WHEN RatecodeID = 2 THEN 'JFK' WHEN RatecodeID = 3 THEN 'Newark' WHEN RatecodeID = 4 THEN 'Nassau or Westchester' WHEN RatecodeID = 5 THEN 'Negotiated fare' WHEN RatecodeID = 6 THEN 'Group ride' ELSE 'Other' END,
  TIME_FORMAT(__time, 'EEEE'),
  TIME_EXTRACT(__time, 'HOUR'),
  AVG(fare_amount / trip_distance) FILTER (WHERE trip_distance <> 0 AND trip_distance IS NOT NULL)
FROM nyctaxi
GROUP BY RatecodeID, TIME_FORMAT(__time, 'EEEE'), TIME_EXTRACT(__time, 'HOUR')
ORDER BY RatecodeID, TIME_FORMAT(__time, 'EEEE'), TIME_EXTRACT(__time, 'HOUR')
```

### 4.**Advanced Date and Time Analysis:** Identify the top 5 busiest hours of the week in terms of total trips.
```sql
SELECT
  TIME_FORMAT(__time, 'EEEE'),
  TIME_EXTRACT(__time, 'HOUR'),
  COUNT(1)
FROM nyctaxi
GROUP BY TIME_FORMAT(__time, 'EEEE'), TIME_EXTRACT(__time, 'HOUR')
ORDER BY COUNT(1) DESC LIMIT 5
```


### 5.**Geospatial Analysis:** Find the shortest path (in terms of distance) between two given locations (PULocationID and DOLocationID) using the available trip data.
```sql
SELECT
  PULocationID,
  DOLocationID,
  MIN(trip_distance) AS minimum_distance
FROM "nyctaxi"
GROUP BY PULocationID, DOLocationID
ORDER BY PULocationID, DOLocationID
```
