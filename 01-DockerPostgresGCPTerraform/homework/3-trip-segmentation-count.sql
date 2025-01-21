/*
Don't know why but i got these numbers:
A: 102,152
B: 194,223
C: 107,772
D: 27,303
E: 34,908
*/

-- A (102_152)
SELECT
	COUNT(*) AS trips_quantity
FROM 
	public."green-tripdata" AS gt
WHERE	
	lpep_pickup_datetime::date >= '2019-10-01'::date AND
	lpep_pickup_datetime::date < '2019-11-01'::date AND
	trip_distance <= 1;
	
-- B (194_223)
SELECT
	COUNT(*) AS trips_quantity
FROM 
	public."green-tripdata" AS gt
WHERE	
	lpep_pickup_datetime::date >= '2019-10-01'::date AND
	lpep_pickup_datetime::date < '2019-11-01'::date AND
	trip_distance > 1 AND trip_distance <= 3;

-- C (107_772)
SELECT
	COUNT(*) AS trips_quantity
FROM 
	public."green-tripdata" AS gt
WHERE	
	lpep_pickup_datetime::date >= '2019-10-01'::date AND
	lpep_pickup_datetime::date < '2019-11-01'::date AND
	trip_distance > 3 AND trip_distance <= 7;

-- D (27_303)
SELECT
	COUNT(*) AS trips_quantity
FROM 
	public."green-tripdata" AS gt
WHERE	
	lpep_pickup_datetime::date >= '2019-10-01'::date AND
	lpep_pickup_datetime::date < '2019-11-01'::date AND
	trip_distance > 7 AND trip_distance <= 10;

-- E (34_908)
SELECT
	COUNT(*) AS trips_quantity
FROM 
	public."green-tripdata" AS gt
WHERE	
	lpep_pickup_datetime::date >= '2019-10-01'::date AND
	lpep_pickup_datetime::date < '2019-11-01'::date AND
	trip_distance > 10;