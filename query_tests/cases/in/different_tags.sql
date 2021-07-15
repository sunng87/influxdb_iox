-- Tests for the correctness when chunks have different tag sets
-- IOX_SETUP: OneMeasurementTwoChunksDifferentTagSet

-- select a column that is not a key
SELECT temp from h2o;

-- select all the different tags and should be filled in a null in different sets
SELECT * from h2o;
