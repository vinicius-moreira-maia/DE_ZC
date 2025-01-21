-- 2019-10-31 -> 515.89 miles
SELECT
	lpep_pickup_datetime::date, 
	MAX(trip_distance) AS longest_trip
FROM 
	public."green-tripdata"
GROUP BY 
	lpep_pickup_datetime::date
HAVING	
	lpep_pickup_datetime::date IN 
	('2019-10-11'::date, '2019-10-24'::date, '2019-10-26'::date, '2019-10-31'::date)
ORDER BY
	longest_trip DESC;