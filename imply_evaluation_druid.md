'''sql
SELECT
  PULocationID,
  DOLocationID,
  MIN(trip_distance) AS minimum_distance
FROM "nyctaxi"
GROUP BY PULocationID, DOLocationID
ORDER BY PULocationID, DOLocationID
'''
'''sql
select TIME_FORMAT(__time,'EEEE'),TIME_EXTRACT(__time,'HOUR'), count(1) from nyctaxi group by TIME_FORMAT(__time,'EEEE'), TIME_EXTRACT(__time,'HOUR') order by count(1)
'''
'''sql
select case when payment_type=1 then 'Credit Card' when payment_type=2 then 'Cash' else 'Other' end as payment_type ,avg(tip_amount) as average_tip_amount from nyctaxi where payment_type in (1,2) group by payment_type
'''