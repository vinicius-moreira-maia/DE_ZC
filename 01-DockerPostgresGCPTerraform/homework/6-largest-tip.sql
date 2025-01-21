-- JFK Airport -> 87.3
SELECT
	tz1."Zone", tz2.dropout_zone_id, tz2.largest_tip
FROM 
	public."taxi-zone" AS tz1
JOIN (SELECT
		gt."DOLocationID" AS dropout_zone_id,
		MAX(gt.tip_amount) AS largest_tip
	  FROM 
		public."green-tripdata" AS gt
	  JOIN 
		public."taxi-zone" AS tz
	  ON
		gt."PULocationID" = tz."LocationID"
	  WHERE
		EXTRACT(MONTH FROM gt.lpep_pickup_datetime::DATE) = 10 AND
		tz."Zone" = 'East Harlem North'
	  GROUP BY
		gt."DOLocationID"
	  ORDER BY
		largest_tip DESC LIMIT 1) AS tz2
ON
	tz1."LocationID" = tz2.dropout_zone_id;