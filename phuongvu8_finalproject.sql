
SELECT *
FROM us_household_income1
;
-- Data cleaning

UPDATE us_household_income1
SET `Type` = 'CDP'
WHERE `Type` = 'CPD';


UPDATE us_household_income1
SET `Type` = 'Borough'
WHERE `Type` = 'Boroughs';

ALTER TABLE us_household_income1 MODIFY area_code INT; -- change the data type of area_code from varchar to int

UPDATE us_household_income1 
SET aland = NULL 
WHERE aland = 0;

UPDATE us_household_income1 
SET awater = NULL 
WHERE awater = 0;

SELECT *
FROM us_household_income1;

-- delete duplicates

CREATE TEMPORARY TABLE temp_ids AS
SELECT MIN(row_id) AS id
FROM us_household_income1
GROUP BY id;

DELETE FROM us_household_income1
WHERE row_id NOT IN (SELECT id FROM temp_ids);

DROP TEMPORARY TABLE temp_ids;

-- đồng bộ dữ liệu

SELECT State_Name
FROM `US_Household_Income1`
where md5(LEFT(REGEXP_REPLACE(State_Name, '\\b([A-Za-z])[^ ]*[ ]?', '$1'), 5)) = md5(lower(LEFT(REGEXP_REPLACE(State_Name, '\\b([A-Za-z])[^ ]*[ ]?', '$1'), 5)));

-- có 2 giá trị state_name chữ đầu viết thường

SELECT State_Name
FROM `US_Household_Income1`
where md5(LEFT(state_name,5)) = md5(lower(LEFT(state_name,5)));

UPDATE US_Household_Income1
SET 
    State_Name = CASE
                    WHEN State_Name REGEXP '^[a-z]' THEN CONCAT(UPPER(LEFT(State_Name, 1)), (SUBSTRING(State_Name, 2)))
                    ELSE State_Name
                 END,
    County = CASE
                WHEN County REGEXP '^[a-z]' THEN CONCAT(UPPER(LEFT(County, 1)), (SUBSTRING(County, 2)))
                ELSE County
             END,
    City = CASE
              WHEN City REGEXP '^[a-z]' THEN CONCAT(UPPER(LEFT(City, 1)), (SUBSTRING(City, 2)))
              ELSE City
           END,
    Place = CASE
               WHEN Place REGEXP '^[a-z]' THEN CONCAT(UPPER(LEFT(Place, 1)), (SUBSTRING(Place, 2)))
               ELSE Place
            END,
    Type = CASE
              WHEN Type REGEXP '^[a-z]' THEN CONCAT(UPPER(LEFT(Type, 1)), (SUBSTRING(Type, 2)))
              ELSE Type
           END;

-- check if zip_code is null or not interger           
SELECT *
FROM US_Household_Income1
WHERE `Zip_Code` IS NULL OR `Zip_Code` NOT REGEXP '^[0-9]+$';

-- check if area_code is null or not 3-digit interger
SELECT *
FROM US_Household_Income1
WHERE area_code IS NULL OR length(area_code) != 3 OR area_code NOT REGEXP '^[0-9]{3}$';

-- check lat, lon is not null or not decimal
SELECT *
FROM US_Household_Income1
WHERE lat IS NULL OR lon IS NULL OR lat NOT REGEXP '^-?[0-9]{1,3}\.[0-9]{1,7}$' OR lon NOT REGEXP '^-?[0-9]{1,3}\.[0-9]{1,7}$';

SELECT *
FROM US_Household_Income1
WHERE lat > 90 OR lat < -90 OR lon > 180 OR lon < -180;

SELECT DISTINCT County
FROM us_household_income1
WHERE County REGEXP "[^a-z \s . ' -]";

-- EDA

-- Task 1: Summarizing Data by State

SELECT state_name, state_ab, AVG(aland), AVG(awater)
FROM us_household_income1
GROUP BY state_name, state_ab
ORDER BY AVG(aland) DESC
;

-- Task 2: Filtering Cities by Population Range

SELECT state_name, city, county
FROM us_household_income1
WHERE aland BETWEEN 50000000 and 100000000
ORDER BY city
;

-- Task 3: Counting Cities per State

SELECT state_name, state_ab, COUNT(DISTINCT(city)) AS number_
FROM us_household_income1
GROUP BY state_name, state_ab
ORDER BY number_ DESC
;

-- Task 4: Identifying Counties with Significant Water Area

SELECT state_name, county, SUM(awater) AS total_
FROM us_household_income1
GROUP BY state_name, county
ORDER BY total_ DESC
LIMIT 10
;

-- Task 5: Finding Cities Near Specific Coordinates

SELECT state_name, city, county, lat, lon
FROM us_household_income1
WHERE lat between 30 and 35 AND lon between -90 and -85
ORDER BY city
;

-- Task 6: Using Window Functions for Ranking

SELECT city, state_name, aland,
RANK() OVER(PARTITION BY state_name ORDER BY aland) AS rank_
FROM us_household_income1
ORDER BY state_name, rank_
;

-- Task 7: Creating Aggregate Reports

SELECT state_name, SUM(aland), SUM(awater), COUNT(DISTINCT(city))
FROM us_household_income1
GROUP BY state_name
ORDER BY SUM(aland) DESC
;

-- Task 8: Subqueries for Detailed Analysis


SELECT state_name, city, aland
FROM us_household_income1
WHERE aland > (SELECT AVG(aland) FROM us_household_income1)
ORDER BY aland DESC
;

-- Task 9: Identifying Cities with High Water to Land Ratios

SELECT * FROM 
(
SELECT state_name, city, aland, awater, awater/aland AS ratio
FROM us_household_income1
) AS t1
WHERE ratio >= 0.5
ORDER BY ratio DESC
;

-- Task 10: Dynamic SQL for Custom Reports

DELIMITER $$
CREATE PROCEDURE p_state_report (p_state_ab VARCHAR(2))
BEGIN
	SELECT COUNT(DISTINCT(city)), AVG(aland)
    FROM us_household_income1
    WHERE state_ab = p_state_ab;
    
    SELECT city, aland, awater
    FROM us_household_income1
    WHERE state_ab = p_state_ab;
END $$
DELIMITER ;

CALL p_state_report ('AL');

-- Task 11: Creating and Using Temporary Tables

CREATE TEMPORARY TABLE tem_
SELECT *
FROM us_household_income1
ORDER BY aland DESC
LIMIT 20
;

SELECT city, state_name, aland, awater,
AVG(awater) OVER()
FROM tem_
;

-- Task 12: Complex Multi-Level Subqueries

SELECT *
FROM 
(
SELECT state_name,  AVG(aland) AS avg_
FROM us_household_income1
GROUP BY state_name
) AS t1
WHERE avg_ > (SELECT AVG(aland) FROM us_household_income1)
;

-- Task 13: Optimizing Indexes for Query Performance

CREATE INDEX ON us_household_income1 (state_name);

CREATE INDEX ON us_household_income1 (city);

CREATE INDEX ON us_household_income1 (county);

-- Task 14: Recursive Common Table Expressions (CTEs)

WITH cte AS 
( SELECT 
	City,
	State_Name,
	SUM(ALand) AS total_aland,
	ROW_NUMBER() OVER(PARTITION BY State_Name ORDER BY City)  AS `rank_`
FROM us_household_income1 
GROUP BY State_Name, City
ORDER BY State_Name, City)
SELECT *,
SUM(total_aland) OVER(PARTITION BY state_name ORDER BY city) AS cur_total
FROM cte
;

-- Task 15: Data Anomalies Detection

WITH cte AS
(
SELECT state_name, city, 
		aland, AVG(aland) OVER(PaRTITION BY state_name) AS avg_state_aland,
		(aland-AVG(aland) OVER(PaRTITION BY state_name))/(STD(aland) OVER(PaRTITION BY state_name)) AS anomaly_score
FROM us_household_income1
)
SELECT *
FROM cte
WHERE anomaly_score NOT BETWEEN -3 AND 3
ORDER BY anomaly_score DESC
 ;
 
 -- Task 16: Stored Procedures for Complex Calculations

DELIMITER $$
CREATE PROCEDURE p_predict(p_aland INT)
BEGIN
	SELECT AVG(aland) OVER (ORDER BY aland ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    FROM predict_proce;
END $$
DELIMITER ;

-- Task 17: Implementing Triggers for Data Integrity

CREATE TABLE summary_table_2
SELECT state_name, SUM(aland) AS total_aland, SUM(awater) AS total_awater
FROM us_household_income1
;

DELIMITER $$
CREATE TRIGGER aa_trigger_summary
	AFTER UPDATE ON us_household_income1
    FOR EACH ROW
BEGIN
	SELECT state_name,
    total_aland = total_aland + NEW.aland
    FROM summary_table_2;
    
    SELECT state_name,
    total_awater = total_awater + NEW.awater
    FROM summary_table_2;
END $$
DELIMITER $$

DELIMITER $$
CREATE TRIGGER aa_trigger_summary
	BEFORE UPDATE ON us_household_income1
    FOR EACH ROW
BEGIN
	SELECT state_name,
    total_aland = total_aland - OLD.aland
    FROM summary_table_2;
    
    SELECT state_name,
    total_awater = total_awater - OLD.awater
    FROM summary_table_2;
END $$
DELIMITER $$

-- Task 18: Advanced Data Encryption and Security

-- Task 19: Geospatial Analysis

DELIMITER $$
CREATE PROCEDURE p_identify_city_2(p_lat decimal(10,7), p_lon decimal(10,7), radius INT)
BEGIN
	SELECT * FROM 
    (
    SELECT city, state_name, county, 
    (6371 * acos( 
                cos( radians(lat) ) 
              * cos( radians( p_lat ) ) 
              * cos( radians( p_lon ) - radians(lon) ) 
              + sin( radians(lat) ) 
              * sin( radians( p_lat ) )
        ) ) as distance 
	FROM US_Household_Income1
    ) AS t1
    WHERE distance <= radius
    ;
END $$
DELIMITER ;

CALL p_identify_city_2(30.31627836, -70.32773, 1000);

-- Task 20: Analyzing Correlations


SELECT (SUM(xy) - SUM(x) * SUM(y) / n) / 
    SQRT((SUM(xx) - SUM(x) * SUM(x) / n) * (SUM(yy) - SUM(y) * SUM(y) / n)) AS correlation 
FROM (
	SELECT 
		aland/10 as x,
        awater/10 as y, 
        aland/10 * aland/10 as xx,
        awater/10 * awater/10 as yy,
        aland/10 * awater/10 as xy,
        count(*) over(PARTITION BY state_name) as n 
	FROM US_Household_Income1
		) as corr_
GROUP BY n; 

-- error 1690: I have not find out the way to handle this problem

DELIMITER $$
CREATE PROCEDURE CalculatePearsonCorrelation(x_col VARCHAR(255), y_col VARCHAR(255), table_name VARCHAR(255))
BEGIN
    SET @sql = CONCAT(
        'WITH stats AS (
            SELECT 
                COUNT(*) AS n,
                SUM(CAST(', x_col, ' AS DECIMAL(30, 2))) AS sum_x,
                SUM(CAST(', y_col, ' AS DECIMAL(30, 2))) AS sum_y,
                SUM(CAST(', x_col, ' AS DECIMAL(30, 2)) * CAST(', y_col, ' AS DECIMAL(30, 2))) AS sum_xy,
                SUM(CAST(', x_col, ' AS DECIMAL(30, 2)) * CAST(', x_col, ' AS DECIMAL(30, 2))) AS sum_x2,
                SUM(CAST(', y_col, ' AS DECIMAL(30, 2)) * CAST(', y_col, ' AS DECIMAL(30, 2))) AS sum_y2
            FROM ', table_name, '
        ),
        correlation AS (
            SELECT
                (n * sum_xy - sum_x * sum_y) / 
                SQRT((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)) AS corr
            FROM
                stats
        )
        SELECT 
            ROUND(corr, 2) AS Pearson_Correlation_Coefficient
        FROM 
            correlation;'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;

CALL `CalculatePearsonCorrelation` ('ALand', 'AWater', 'US_Household_Income');

-- Task 21: Hotspot Detection

WITH cte AS
(
SELECT state_name, city, 
		aland, awater,
		ABS((aland-AVG(aland) OVER(PARTITION BY state_name))/(STD(aland) OVER(PARTITION BY state_name)))
        + ABS((awater-AVG(awater) OVER(PARTITION BY state_name))/(STD(awater) OVER(PARTITION BY state_name))) AS deviation_score
FROM us_household_income1
)
SELECT *
FROM cte
ORDER BY deviation_score DESC
 ;
 
 WITH cte1 AS
(
SELECT state_name, city, 
		aland, awater,
		ABS((aland-AVG(aland) OVER())/(STD(aland) OVER()))
        + ABS((awater-AVG(awater) OVER())/(STD(awater) OVER())) AS deviation_score
FROM us_household_income1
)
SELECT *
FROM cte1
ORDER BY deviation_score DESC
 ;
 
 WITH Stats AS (
    SELECT 
        AVG(ALand) AS Avg_Land_Area,
        STDDEV(ALand) AS StdDev_Land_Area,
        AVG(AWater) AS Avg_Water_Area,
        STDDEV(AWater) AS StdDev_Water_Area
    FROM 
        US_Household_Income
),
Hotspots AS (
    SELECT 
        City,
        State_Name,
        ALand,
        AWater,
        (ABS(ALand - (SELECT Avg_Land_Area FROM Stats)) / (SELECT StdDev_Land_Area FROM Stats)) AS Land_Area_Z_Score,
        (ABS(AWater - (SELECT Avg_Water_Area FROM Stats)) / (SELECT StdDev_Water_Area FROM Stats)) AS Water_Area_Z_Score
    FROM 
        US_Household_Income
)
SELECT 
    City,
    State_Name,
    ALand,
    AWater,
    ROUND((Land_Area_Z_Score + Water_Area_Z_Score), 2) AS Deviation_Score
FROM 
    Hotspots
ORDER BY 
    Deviation_Score DESC;
 
 -- Task 22: Resource Allocation Optimization
 
SELECT state_name, city, aland, awater, 
	(aland + awater)/(SUM(aland) OVER() + SUM(awater) OVER()) AS allocated
FROM us_household_income1
ORDER BY allocated DESC
;

WITH ResourceAllocation AS (
    SELECT 
        City,
        State_Name,
        ALand,
        AWater,
        (ALand + AWater) / SUM(ALand + AWater) OVER () AS Resource_Allocation_Ratio
    FROM 
        US_Household_Income
)
SELECT 
    City,
    State_Name,
    ALand,
    AWater,
    Resource_Allocation_Ratio AS Allocated_Resources
FROM 
    ResourceAllocation
ORDER BY 
    Allocated_Resources DESC;

SELECT *
FROM us_household_income1;
 