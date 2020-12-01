CREATE TABLE raw_records 
(
  TAGS           NVARCHAR(65535),
  records_text   NVARCHAR(65535) NOT NULL,
  partition_id   VARCHAR NOT NULL
);