-- Посмотреть 5 случайных строк
SELECT * FROM user_logs ORDER BY RANDOM() LIMIT 5;
-- Замена запятых на точки
--UPDATE user_logs SET s_a_submission_status_viewed_avg = REPLACE(s_a_submission_status_viewed_avg, ',', '.') WHERE s_a_submission_status_viewed_avg LIKE '%,%';
-- Изменение типа колонки
--ALTER TABLE user_logs ALTER COLUMN s_a_submission_status_viewed_avg TYPE REAL USING s_a_submission_status_viewed_avg::REAL;

--SELECT AVG(s_course_viewed_avg) AS average_s_course_viewed_avg FROM user_logs;