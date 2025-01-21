-- East Harlem North -> +18_686
-- East Harlem South -> +16_797
-- Morningside Heights -> +13_029

SELECT
	tz."Zone",
	SUM(gt.total_amount) AS sum_total_amount
FROM 
	public."green-tripdata" AS gt
JOIN 
	public."taxi-zone" AS tz
ON
	gt."PULocationID" = tz."LocationID"
WHERE
	gt.lpep_pickup_datetime::DATE = '2019-10-18'::DATE
GROUP BY
	tz."Zone"
HAVING
	SUM(gt.total_amount) > 13000;
	