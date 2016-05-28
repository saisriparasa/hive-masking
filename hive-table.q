DROP TABLE IF EXISTS pageviews;

CREATE EXTERNAL TABLE pageviews(
  code STRING,
  page STRING,
  views INT,
  bytes STRING
)
PARTITIONED BY(month STRING, dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

ALTER TABLE pageviews ADD PARTITION(month='04',dt='12') LOCATION 's3://aws-sai-sriparasa/data/sqoop/hive/partition_sample/2016/04/';
ALTER TABLE pageviews ADD PARTITION(month='05',dt='01') LOCATION 's3://aws-sai-sriparasa/data/sqoop/hive/partition_sample/2016/05/';
