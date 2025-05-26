-- Create database for disaster response system
CREATE DATABASE IF NOT EXISTS disaster_response;
USE disaster_response;

-- Create external table for raw tweets
CREATE EXTERNAL TABLE IF NOT EXISTS raw_tweets (
  text STRING,
  location STRING,
  disaster_type STRING,
  severity INT,
  coordinates STRUCT<lat:DOUBLE, lng:DOUBLE>,
  timestamp STRING,
  user_id STRING,
  retweet_count INT,
  verified_report BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/disaster_response/raw_tweets/';

-- Create a partitioned table for optimized queries
CREATE TABLE IF NOT EXISTS disaster_events (
  text STRING,
  location STRING,
  severity INT,
  coordinates STRUCT<lat:DOUBLE, lng:DOUBLE>,
  timestamp STRING,
  user_id STRING,
  retweet_count INT,
  verified_report BOOLEAN
)
PARTITIONED BY (disaster_type STRING)
STORED AS ORC;

-- Insert data from raw tweets into partitioned table
INSERT OVERWRITE TABLE disaster_events PARTITION(disaster_type)
SELECT 
  text, 
  location, 
  severity, 
  coordinates, 
  timestamp, 
  user_id, 
  retweet_count, 
  verified_report,
  disaster_type
FROM raw_tweets;

-- Create a table for aggregated disaster reports by location
CREATE TABLE IF NOT EXISTS disaster_summary (
  location STRING,
  disaster_type STRING,
  event_count INT,
  avg_severity DOUBLE,
  verified_count INT,
  last_updated STRING
);

-- Populate the summary table
INSERT OVERWRITE TABLE disaster_summary
SELECT 
  location,
  disaster_type,
  COUNT(*) as event_count,
  AVG(severity) as avg_severity,
  SUM(CASE WHEN verified_report = true THEN 1 ELSE 0 END) as verified_count,
  MAX(timestamp) as last_updated
FROM disaster_events
GROUP BY location, disaster_type;

-- Sample queries:

-- 1. Find most affected locations
-- SELECT location, COUNT(*) as event_count 
-- FROM disaster_events 
-- GROUP BY location 
-- ORDER BY event_count DESC;

-- 2. Find high severity events (severity >= 4)
-- SELECT * FROM disaster_events 
-- WHERE severity >= 4;

-- 3. Find recent events in the last hour
-- SELECT * FROM disaster_events 
-- WHERE from_unixtime(unix_timestamp(timestamp)) > 
--       from_unixtime(unix_timestamp() - 3600);