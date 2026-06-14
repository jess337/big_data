-- Посмотреть 5 случайных строк
--SELECT * FROM user_logs ORDER BY RANDOM() LIMIT 5;
-- Замена запятых на точки
--UPDATE user_logs SET namer_level = REPLACE(namer_level, ',', '.') WHERE namer_level LIKE '%,%';
-- Изменение типа колонки
--ALTER TABLE user_logs ALTER COLUMN namer_level TYPE REAL USING namer_level::REAL;
--SELECT column_name FROM information_schema.columns WHERE table_name = 'user_logs';
--SELECT AVG(s_course_viewed_avg) AS average_s_course_viewed_avg FROM user_logs;
--SELECT AVG(namer_level) FROM user_logs
--where userid =(
--select userid FROM user_logs
--GROUP BY userid
--ORDER BY SUM(s_all)
--LIMIT 1);
--SELECT COUNT(*) FROM dmr.analytics_student_performance;

--SELECT COUNT(*) FROM dmr.analytics_student_performance;

DROP SCHEMA IF EXISTS dmr CASCADE;

--SELECT COUNT(*) FROM dmr.analytics_student;

--SELECT * 
--FROM dmr.analytics_student_performance 
--LIMIT 10;

--SELECT activity_category, COUNT(*) 
--FROM dmr.analytics_student_performance 
--GROUP BY activity_category;

