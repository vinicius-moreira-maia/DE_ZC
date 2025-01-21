select * from yellow_taxi_trips limit 3;
select * from zone_lookup;

-- o maior valor pago de passageiro que pegou taxi no Brooklyn
SELECT 
	MAX(ytt.total_amount)
FROM 
	yellow_taxi_trips ytt 
JOIN 
	zone_lookup zl
ON
	ytt."PULocationID" = zl."LocationID"
WHERE
	zl."Borough" = 'Brooklyn';
 
-- ninguém teve uma corrida iniciada nestes bairros
SELECT DISTINCT
	zl."Borough"
FROM 
	yellow_taxi_trips ytt 
RIGHT JOIN 
	zone_lookup zl
ON
	ytt."PULocationID" = zl."LocationID"
WHERE 	
	ytt."PULocationID" IS NULL;

-- ninguém foi deixado nestes bairros
SELECT 
	zl."Borough"
FROM
	zone_lookup zl 
LEFT JOIN 
	yellow_taxi_trips ytt
ON
	ytt."PULocationID" = zl."LocationID"
WHERE 	
	ytt."DOLocationID" IS NULL;

-- zonas cujas corridas tiveram uma média maior que 50 em todo o periodo
SELECT  
	zl."Zone", AVG(ytt.total_amount) as avg_price 
FROM
	zone_lookup zl 
JOIN 
	yellow_taxi_trips ytt
ON
	ytt."PULocationID" = zl."LocationID"
GROUP BY
	zl."Zone"
HAVING
	AVG(ytt.total_amount) > 50
ORDER BY
	avg_price
DESC;

-- outra forma de checar as zonas não presentes nas corridas (subconsulta)
SELECT 
	* 
FROM
	zone_lookup zl
WHERE "LocationID" NOT IN (SELECT "PULocationID" FROM yellow_taxi_trips);

-- extraindo o mês da data passada como argumento
SELECT EXTRACT(MONTH FROM DATE '2021-01-01') AS month;

-- zonas cujas corridas tiveram uma média maior que 50 em janeiro
SELECT  
    zl."Zone", 
    AVG(ytt.total_amount) AS avg_price 
FROM
    zone_lookup zl 
JOIN 
    yellow_taxi_trips ytt
ON
    ytt."PULocationID" = zl."LocationID"
WHERE
    EXTRACT(MONTH FROM TO_TIMESTAMP(ytt.tpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS')) = 1
GROUP BY
    zl."Zone"
HAVING
    AVG(ytt.total_amount) > 50 
ORDER BY
    avg_price DESC;
