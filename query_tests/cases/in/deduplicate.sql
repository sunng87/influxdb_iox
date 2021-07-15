-- Test the correctness of deduplicating logic
-- IOX_SETUP: OneMeasurementThreeChunksWithDuplicates

select time, state, city, min_temp, max_temp, area from h2o order by time, state, city;
