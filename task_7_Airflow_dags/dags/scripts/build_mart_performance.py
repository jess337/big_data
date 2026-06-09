# dags/scripts/build_mart_performance.py
# Этот скрипт строит расширенную аналитическую витрину dmr.analytics_student_performance.
# В отличие от простой витрины, здесь рассчитываются метрики активности:
# количество событий, просмотры, отправки заданий, стабильность, категория активности и др.

# Импортируем модуль os для чтения переменных окружения (настройки подключения к БД)
import os
# Импортируем sys для аварийного выхода при критической ошибке
import sys
# Импортируем psycopg2 — адаптер PostgreSQL для Python
import psycopg2
# Импортируем sql для безопасного формирования SQL-запросов (защита от инъекций)
from psycopg2 import sql
# Импортируем execute_values для высокопроизводительной массовой вставки (пакетами)
from psycopg2.extras import execute_values

def get_db_config():
    """
    Считывает параметры подключения к базе данных из переменных окружения.
    Эти переменные должны быть определены в контейнере Airflow (через .env файл или docker-compose).
    Возвращает словарь, пригодный для передачи в psycopg2.connect(**config).
    """
    config = {
        'host': os.getenv('DB_HOST'),       # Адрес сервера БД (например, host.docker.internal)
        'port': os.getenv('DB_PORT'),       # Порт (у вас 5433)
        'database': os.getenv('DB_NAME'),   # Имя базы данных (my_db_Korchagina)
        'user': os.getenv('DB_USER'),       # Пользователь (Korchagina)
        'password': os.getenv('DB_PASSWORD') # Пароль
    }
    return config

def create_mart_performance():
    """
    Главная функция построения расширенной витрины.
    Выполняет:
      1. Подключение к учебной БД.
      2. Создание схемы dmr (если отсутствует).
      3. Создание таблицы analytics_student_performance (если отсутствует).
      4. Агрегацию данных из user_logs (события по неделям) и departments,
         расчёт всех метрик, вставку/обновление записей (UPSERT).
    """
    conn = None
    try:
        # Получаем настройки подключения
        config = get_db_config()
        print(f"Подключение к {config['host']}:{config['port']} ...")

        # Устанавливаем соединение с PostgreSQL
        conn = psycopg2.connect(**config)
        # Отключаем автоматический коммит — управляем транзакциями вручную для целостности
        conn.autocommit = False

        # ================== 1. Создание схемы dmr ==================
        # Схема — это логическое пространство для группировки таблиц витрин.
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
            conn.commit()   # Фиксируем создание схемы
        print("Схема dmr создана/существует.")

        # ================== 2. Создание таблицы витрины ==================
        # Таблица содержит все требуемые поля: от идентификаторов до агрегированных метрик.
        # Первичный ключ — (student_id, course_id) — обеспечивает уникальность пары студент-курс.
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

        # ================== 3. Заполнение данными (сложная агрегация) ==================
        # SQL-запрос разбит на несколько CTE (Common Table Expressions) для наглядности.
        # Каждый этап последовательно преобразует сырые логи в итоговые строки витрины.
        sql_insert = """
        -- CTE 1: weekly_agg — подготовка сырых данных.
        --   - отбираем только строки с непустой оценкой (namer_level)
        --   - преобразуем текстовые поля leveled и name_osno в целые числа (через приведение)
        --   - оставляем только те строки, где leveled и name_osno состоят из цифр (регулярка '^[0-9]+$')
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
        -- CTE 2: student_week_stats — явное приведение оценки к INTEGER.
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
        -- CTE 3: aggregated — агрегация по студенту и курсу.
        --   - MAX для полей, которые не меняются в рамках пары (кафедра, семестр, курс, оценка, уровень, основа)
        --   - SUM для накопительных метрик (суммарные события, просмотры, отправки)
        --   - AVG для среднего числа событий в неделю
        --   - array_agg с сортировкой для получения недели с максимальной активностью
        --   - consistency_score = доля недель, где была активность (s_all > 0)
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
        -- CTE 4: with_department — присоединяем название кафедры из справочника departments
        --   и преобразуем числовые коды уровня образования и основы обучения в понятные строки.
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
        -- CTE 5: final_data — финальные вычисления: округление, категория активности.
        --   - ROUND для средних и коэффициента стабильности
        --   - Категория: низкая (<100 событий), средняя (100-299), высокая (≥300)
        --   - Отсекаем строки с невалидными оценками (оставляем только 2,3,4,5)
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
        -- Основной INSERT с конфликтным обновлением (UPSERT).
        -- Если запись с таким (student_id, course_id) уже существует, обновляем все поля,
        -- кроме первичного ключа, и проставляем новое время last_update.
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

        # Выполняем составной SQL-запрос
        with conn.cursor() as cur:
            cur.execute(sql_insert)
            conn.commit()   # Фиксируем транзакцию
            print(f"Витрина заполнена. Затронуто строк: {cur.rowcount}")  # rowcount — количество вставленных/обновлённых строк

        print("Витрина dmr.analytics_student_performance успешно создана/обновлена.")

    except Exception as e:
        # В случае любой ошибки выводим сообщение, откатываем транзакцию (если соединение открыто)
        # и пробрасываем исключение выше, чтобы Airflow зафиксировал неудачу задачи.
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Гарантированно закрываем соединение с БД, освобождая ресурсы.
        if conn:
            conn.close()

# Если скрипт запущен напрямую (например, для тестирования), а не импортирован как модуль,
# выполняем функцию create_mart_performance().
if __name__ == "__main__":
    create_mart_performance()