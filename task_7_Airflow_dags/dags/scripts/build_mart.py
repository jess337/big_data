# dags/scripts/build_mart.py
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

def create_mart():
    """Основная функция создания простой витрины."""
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

        # 2. Создание таблицы dmr.analytics_student
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dmr.analytics_student (
            student_id     INTEGER NOT NULL,
            course_id      INTEGER NOT NULL,
            department_id  INTEGER,
            semester       INTEGER,
            course_year    INTEGER,
            final_grade    INTEGER CHECK (final_grade IN (2,3,4,5)),
            last_update    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (student_id, course_id)
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        print("Таблица dmr.analytics_student создана/существует.")

        # 3. Заполнение данными (UPSERT)
        select_query = """
        WITH student_final AS (
            SELECT 
                userid,
                courseid,
                MAX(depart) AS department_id,
                MAX(num_sem) AS semester,
                MAX(kurs) AS course_year,
                MAX(namer_level::INTEGER) AS final_grade
            FROM public.user_logs
            WHERE namer_level IS NOT NULL
              AND namer_level IN ('2','3','4','5')
            GROUP BY userid, courseid
        )
        SELECT 
            userid,
            courseid,
            department_id,
            semester,
            course_year,
            final_grade
        FROM student_final;
        """

        insert_query = sql.SQL("""
            INSERT INTO dmr.analytics_student 
            (student_id, course_id, department_id, semester, course_year, final_grade)
            VALUES %s
            ON CONFLICT (student_id, course_id) DO UPDATE SET
                department_id = EXCLUDED.department_id,
                semester      = EXCLUDED.semester,
                course_year   = EXCLUDED.course_year,
                final_grade   = EXCLUDED.final_grade,
                last_update   = CURRENT_TIMESTAMP;
        """)

        with conn.cursor() as cur:
            cur.execute(select_query)
            rows = cur.fetchall()
            if rows:
                data_tuples = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
                execute_values(cur, insert_query, data_tuples, page_size=1000)
                conn.commit()
                print(f"Витрина обновлена. Добавлено/обновлено записей: {len(data_tuples)}")
            else:
                print("Нет данных для вставки.")

        print("Витрина dmr.analytics_student успешно создана/обновлена.")

    except Exception as e:
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_mart()