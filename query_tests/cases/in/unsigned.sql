-- Test basic SQL functionality of unsigned type
-- IOX_SETUP: TwoMeasurementsUnsignedType

-- count has type "unsigned" in line protocol
SELECT town, count from restaurant;

SELECT town, count from school
