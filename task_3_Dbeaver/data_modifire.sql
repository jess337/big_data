-- Посмотреть 5 случайных строк
-- SELECT * FROM user_logs ORDER BY RANDOM() LIMIT 5;
-- Замена запятых на точки
UPDATE user_logs SET s_all_avg = REPLACE(s_all_avg, ',', '.') WHERE s_all_avg LIKE '%,%';
-- Изменение типа колонки
ALTER TABLE user_logs ALTER COLUMN s_all_avg TYPE REAL USING s_all_avg::REAL;
SELECT AVG(s_all_avg) AS average_s_all_avg FROM user_logs;