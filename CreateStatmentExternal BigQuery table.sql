-- Create the external table
CREATE EXTERNAL TABLE IF NOT EXISTS "business.worst_ten_performing_devices"
OPTIONS (
  format_type='PARQUET',
  uris=['gs://ten_worst_performing_devices/*.parquet'],
  partitioning_type='week'
);