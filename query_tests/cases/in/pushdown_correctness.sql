-- Test the correctness of queries when predicates are pushed down
-- IOX_SETUP: TwoMeasurementsPredicatePushDown

-- Test 1: Select everything
SELECT * from restaurant;

-- Test 2: One push-down expression: count > 200
SELECT * from restaurant where count > 200;

-- Test 3: Two push-down expression: count > 200 and town != 'tewsbury'
SELECT * from restaurant where count > 200 and town != 'tewsbury';

-- Test 4: Still two push-down expression: count > 200 and town != 'tewsbury'
SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence');

-- Test 5: three push-down expression: count > 200 and town != 'tewsbury' and count < 40000
SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence') and count < 40000;

-- Test 6: two push-down expression: count > 200 and count < 40000
SELECT * from restaurant where count > 200  and count < 40000;

-- Test 7: two push-down expression on float: system > 4.0 and system < 7.0
SELECT * from restaurant where system > 4.0 and system < 7.0;

-- Test 8: two push-down expression on float: system > 5.0 and system < 7.0
SELECT * from restaurant where system > 5.0 and system < 7.0;

-- Test 9: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
SELECT * from restaurant where system > 5.0 and town != 'tewsbury' and 7.0 > system;

-- Test 10: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and (count = 632 or town = 'reading');

-- Test 11: four push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0 and
SELECT * from restaurant where 5.0 < system and town != 'tewsbury' and system < 7.0 and (count = 632 or town = 'reading') and time > to_timestamp('1970-01-01T00:00:00.000000130+00:00');

--TODO: Hit stackoverflow in DF. Ticket https://github.com/apache/arrow-datafusion/issues/419
-- Test 12: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0 and town = 'reading'
-- SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and town = 'reading'",
-- let expected = vec![
-- "+-------+--------+-------------------------------+---------+",
-- "| count | system | time                          | town    |",
-- "+-------+--------+-------------------------------+---------+",
-- "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading |",
-- "+-------+--------+-------------------------------+---------+",

-- Test 13: three push-down expression: system > 5.0 and system < 7.0 and town = 'reading'
SELECT * from restaurant where system > 5.0 and system < 7.0 and town = 'reading';
