-- Test basic SQL functionality
-- IOX_SETUP: TwoMeasurements

-- if we can not run this we are totally hosed
SELECT * from cpu;

-- basic projection
SELECT user, region from cpu;

-- basic predicate
SELECT * from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00');

-- projection + predicate
SELECT user, region from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00');

-- basic grouping
SELECT count(*) from cpu group by region;

-- select from the second measurement
SELECT * from disk;
