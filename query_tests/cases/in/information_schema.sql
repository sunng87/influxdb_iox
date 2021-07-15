-- Test information schema integration
-- IOX_SETUP: TwoMeasurementsManyFields

-- Validate the information schema is hooked up
SELECT * from information_schema.tables;

-- and that basic predicates work
SELECT * from information_schema.columns where table_name = 'h2o' OR table_name = 'o2' order by table_name;

-- validate we have access to SHOW SCHEMA for listing columns
SHOW COLUMNS FROM h2o;
