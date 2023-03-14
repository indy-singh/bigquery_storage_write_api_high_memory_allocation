CREATE TABLE project_name.dataset_name.table_name (
	-- fields
    minute_bucket timestamp,
    user_reference string,
    system_id int64,
	application_name string,
	request_type_name string,
	status_code int64,
	revision int64,
	host_name string,
	external_application_name string,
	ip_address string,
	--counters
    total_duration int64,
	total_square_duration int64,
	request_bytes int64,
	response_bytes int64,
	pg_sessions int64,
	sql_sessions int64,
	pg_statements int64,
	sql_statements int64,
	hits int64,
	cassandra_statements int64,
	pg_entities int64,
	sql_entities int64
)
PARTITION BY TIMESTAMP_TRUNC(minute_bucket, HOUR)
CLUSTER BY system_id, user_reference
OPTIONS(  
  partition_expiration_days=166,
  require_partition_filter=true
  );