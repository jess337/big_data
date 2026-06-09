# dags/scripts/build_mart_performance.py
import os
import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

def get_db_config():
    """Читает параметры подключения из переменных окружения."""
    config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    return config

def create_mart_performance():
    """Создаёт и заполняет витрину dmr.analytics_student_performance."""
    conn = None
    try:
        config = get_db_config()
        print(f"Подключение к {config['host']}:{config['port']} ...")
        conn = psycopg2.connect(**config)
        conn.autocommit = False

        # 1. Создание схемы dmr
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
            conn.commit()
        print("Схема dmr создана/существует.")

        # 2. Создание таблицы (ваш код из лабораторной)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
            student_id          INTEGER NOT NULL,
            course_id           INTEGER NOT NULL,
            department_id       INTEGER,
            department_name     VARCHAR,
            education_level     VARCHAR,
            education_base      VARCHAR,
            semester            INTEGER,
            course_year         INTEGER,
            final_grade         INTEGER,
            total_events        INTEGER,
            avg_weekly_events   DECIMAL(10,2),
            total_course_views  INTEGER,
            total_quiz_views    INTEGER,
            total_module_views  INTEGER,
            total_submissions   INTEGER,
            peak_activity_week  INTEGER,
            consistency_score   DECIMAL(5,2),
            activity_category   VARCHAR,
            last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (student_id, course_id)
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        print("Таблица dmr.analytics_student_performance создана/существует.")

        # 3. Заполнение данными (ваш же запрос fill_data_mart)
        sql_insert = """
        WITH weekly_agg AS (
            SELECT 
                userid,
                courseid,
                num_week,
                depart,
                num_sem,
                kurs,
                namer_level,
                leveled::INTEGER AS leveled_int,
                name_osno::INTEGER AS name_osno_int,
                s_all,
                s_course_viewed,
                s_q_attempt_viewed,
                s_a_course_module_viewed,
                s_a_submission_status_viewed
            FROM public.user_logs
            WHERE namer_level IS NOT NULL
              AND leveled ~ '^[0-9]+$'
              AND name_osno ~ '^[0-9]+$'
        ),
        student_week_stats AS (
            SELECT 
                userid,
                courseid,
                depart,
                num_sem,
                kurs,
                CAST(namer_level AS INTEGER) AS final_grade,
                leveled_int,
                name_osno_int,
                num_week,
                s_all,
                s_course_viewed,
                s_q_attempt_viewed,
                s_a_course_module_viewed,
                s_a_submission_status_viewed
            FROM weekly_agg
        ),
        aggregated AS (
            SELECT 
                userid AS student_id,
                courseid AS course_id,
                MAX(depart) AS department_id,
                MAX(num_sem) AS semester,
                MAX(kurs) AS course_year,
                MAX(final_grade) AS final_grade,
                MAX(leveled_int) AS level_ed,
                MAX(name_osno_int) AS name_osno,
                SUM(s_all) AS total_events,
                AVG(s_all) AS avg_weekly_events,
                SUM(s_course_viewed) AS total_course_views,
                SUM(s_q_attempt_viewed) AS total_quiz_views,
                SUM(s_a_course_module_viewed) AS total_module_views,
                SUM(s_a_submission_status_viewed) AS total_submissions,
                (array_agg(num_week ORDER BY s_all DESC))[1] AS peak_activity_week,
                COUNT(CASE WHEN s_all > 0 THEN 1 END) * 1.0 / COUNT(*) AS consistency_score
            FROM student_week_stats
            GROUP BY userid, courseid
        ),
        with_department AS (
            SELECT 
                a.*,
                d.name AS department_name,
                CASE 
                    WHEN a.level_ed = 1 THEN 'бакалавриат'
                    WHEN a.level_ed = 2 THEN 'магистратура'
                    WHEN a.level_ed = 3 THEN 'специалитет'
                    WHEN a.level_ed = 4 THEN 'аспирантура'
                    ELSE 'не указано'
                END AS education_level,
                CASE 
                    WHEN a.name_osno = 1 THEN 'бюджет'
                    WHEN a.name_osno = 2 THEN 'контракт'
                    ELSE 'не указано'
                END AS education_base
            FROM aggregated a
            LEFT JOIN public.departments d ON a.department_id = d.id
        ),
        final_data AS (
            SELECT 
                student_id,
                course_id,
                department_id,
                department_name,
                education_level,
                education_base,
                semester,
                course_year,
                final_grade,
                total_events,
                ROUND(avg_weekly_events, 2) AS avg_weekly_events,
                total_course_views,
                total_quiz_views,
                total_module_views,
                total_submissions,
                peak_activity_week,
                ROUND(consistency_score, 2) AS consistency_score,
                CASE
                    WHEN total_events < 100 THEN 'низкая'
                    WHEN total_events < 300 THEN 'средняя'
                    ELSE 'высокая'
                END AS activity_category
            FROM with_department
            WHERE final_grade IN (2,3,4,5)
        )
        INSERT INTO dmr.analytics_student_performance (
            student_id, course_id, department_id, department_name,
            education_level, education_base, semester, course_year,
            final_grade, total_events, avg_weekly_events,
            total_course_views, total_quiz_views, total_module_views,
            total_submissions, peak_activity_week, consistency_score,
            activity_category, last_update
        )
        SELECT 
            student_id, course_id, department_id, department_name,
            education_level, education_base, semester, course_year,
            final_grade, total_events, avg_weekly_events,
            total_course_views, total_quiz_views, total_module_views,
            total_submissions, peak_activity_week, consistency_score,
            activity_category, CURRENT_TIMESTAMP
        FROM final_data
        ON CONFLICT (student_id, course_id) DO UPDATE SET
            department_id = EXCLUDED.department_id,
            department_name = EXCLUDED.department_name,
            education_level = EXCLUDED.education_level,
            education_base = EXCLUDED.education_base,
            semester = EXCLUDED.semester,
            course_year = EXCLUDED.course_year,
            final_grade = EXCLUDED.final_grade,
            total_events = EXCLUDED.total_events,
            avg_weekly_events = EXCLUDED.avg_weekly_events,
            total_course_views = EXCLUDED.total_course_views,
            total_quiz_views = EXCLUDED.total_quiz_views,
            total_module_views = EXCLUDED.total_module_views,
            total_submissions = EXCLUDED.total_submissions,
            peak_activity_week = EXCLUDED.peak_activity_week,
            consistency_score = EXCLUDED.consistency_score,
            activity_category = EXCLUDED.activity_category,
            last_update = CURRENT_TIMESTAMP;
        """

        with conn.cursor() as cur:
            cur.execute(sql_insert)
            conn.commit()
            print(f"Витрина заполнена. Затронуто строк: {cur.rowcount}")

        print("Витрина dmr.analytics_student_performance успешно создана/обновлена.")

    except Exception as e:
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_mart_performance()