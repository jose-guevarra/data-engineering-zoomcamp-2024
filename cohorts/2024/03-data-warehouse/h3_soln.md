

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `able-door-412405.hw3.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://jose-nyc-tlc-data/green-taxi-2022/green_tripdata_2022-*.parquet']
);

SELECT COUNT(*) FROM `able-door-412405.hw3.external_green_tripdata`;


-- Creating normal table referring to gcs path
CREATE OR REPLACE TABLE `able-door-412405.hw3.green_tripdata_nonpartition` AS
SELECT * FROM `able-door-412405.hw3.external_green_tripdata`;

SELECT COUNT(*) FROM `able-door-412405.hw3.green_tripdata_nonpartition`;


-- Get distinct PULocationID from external table
SELECT DISTINCT `PULocationID` FROM `able-door-412405.hw3.external_green_tripdata`;



-- Get distinct PULocationID from table
SELECT DISTINCT `PULocationID` FROM `able-door-412405.hw3.green_tripdata_nonpartition`;


-- How many records with fare_amount = 0
SELECT COUNT(*) FROM `able-door-412405.hw3.green_tripdata_nonpartition` WHERE `fare_amount` = 0;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `able-door-412405.hw3.green_tripdata_partition`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `able-door-412405.hw3.external_green_tripdata`;



-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `able-door-412405.hw3.green_tripdata_partition_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `able-door-412405.hw3.external_green_tripdata`;


SELECT DISTINCT(PULocationID)
FROM `able-door-412405.hw3.green_tripdata_nonpartition`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


SELECT DISTINCT(PULocationID)
FROM `able-door-412405.hw3.green_tripdata_partition`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';