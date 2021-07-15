-- test of system.operations
-- IOX_SETUP: TwoMeasurementsManyFieldsLifecycle

SELECT
  id,
  status, CAST(cpu_time_used AS BIGINT) > 0 as took_cpu_time,
  CAST(wall_time_used AS BIGINT) > 0 as took_wall_time,
  partition_key,
  chunk_id,
  description
FROM
  system.operations;
