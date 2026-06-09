# dags/scripts/build_mart.py
# Этот скрипт выполняет построение упрощённой аналитической витрины dmr.analytics_student.
# Он подключается к базе данных, создаёт схему dmr и таблицу витрины,
# заполняет её агрегированными данными из таблицы public.user_logs.

# Импортируем модуль os для работы с переменными окружения (чтение настроек подключения)
import os
# Импортируем sys для выхода из программы в случае критической ошибки
import sys
# Импортируем psycopg2 — библиотеку для подключения к PostgreSQL и выполнения SQL-запросов
import psycopg2
# Импортируем sql из psycopg2 для безопасного формирования SQL-запросов (избегаем инъекций)
from psycopg2 import sql
# Импортируем execute_values для массовой вставки данных (пакетный режим, ускоряет работу)
from psycopg2.extras import execute_values

def get_db_config():
    """
    Читает параметры подключения к базе данных из переменных окружения.
    Эти переменные должны быть определены в системе (например, через .env файл или Docker).
    Возвращает словарь с настройками для psycopg2.connect().
    """
    config = {
        'host': os.getenv('DB_HOST'),         # Хост базы данных 
        'port': os.getenv('DB_PORT'),         # Порт 
        'database': os.getenv('DB_NAME'),     # Имя базы данных 
        'user': os.getenv('DB_USER'),         # Пользователь БД 
        'password': os.getenv('DB_PASSWORD')  # Пароль
    }
    return config

def create_mart():
    """
    Основная функция построения витрины dmr.analytics_student.
    Выполняет последовательно:
      1. Подключение к БД.
      2. Создание схемы dmr (если её нет).
      3. Создание таблицы analytics_student (если её нет).
      4. Агрегацию данных из user_logs и загрузку в таблицу (UPSERT).
    """
    conn = None  # Переменная для хранения соединения с БД
    try:
        # Получаем параметры подключения
        config = get_db_config()
        print(f"Подключение к {config['host']}:{config['port']} ...")

        # Устанавливаем соединение с PostgreSQL
        conn = psycopg2.connect(**config)
        # Отключаем автоматический коммит — будем управлять транзакциями вручную
        conn.autocommit = False

        # ================== 1. Создание схемы dmr ==================
        with conn.cursor() as cur:
            # CREATE SCHEMA IF NOT EXISTS — создаст схему, если она не существует
            cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
            conn.commit()   # Фиксируем изменения
        print("Схема dmr создана/существует.")

        # ================== 2. Создание таблицы dmr.analytics_student ==================
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dmr.analytics_student (
            student_id     INTEGER NOT NULL,                     -- ID студента (обязательное поле)
            course_id      INTEGER NOT NULL,                     -- ID курса (обязательное поле)
            department_id  INTEGER,                              -- ID кафедры (может быть NULL)
            semester       INTEGER,                              -- Номер семестра
            course_year    INTEGER,                              -- Курс обучения (1,2,3,4)
            final_grade    INTEGER CHECK (final_grade IN (2,3,4,5)), -- Оценка (только 2-5)
            last_update    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Время последнего обновления записи
            PRIMARY KEY (student_id, course_id)                  -- Составной первичный ключ
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        print("Таблица dmr.analytics_student создана/существует.")

        # ================== 3. Заполнение данными (UPSERT) ==================
        # Сначала формируем запрос на выборку агрегированных данных.
        # Используем CTE (Common Table Expression) для читаемости.
        select_query = """
        WITH student_final AS (
            SELECT 
                userid,
                courseid,
                MAX(depart) AS department_id,               -- Берём кафедру (максимум, т.к. она одна)
                MAX(num_sem) AS semester,                   -- Номер семестра
                MAX(kurs) AS course_year,                   -- Курс обучения
                MAX(namer_level::INTEGER) AS final_grade    -- Преобразуем текстовую оценку в число
            FROM public.user_logs
            WHERE namer_level IS NOT NULL                   -- Только строки с оценкой
              AND namer_level IN ('2','3','4','5')          -- Оставляем только валидные оценки
            GROUP BY userid, courseid                       -- Группируем по паре (студент, курс)
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

        # Запрос на вставку/обновление (UPSERT). 
        # VALUES %s будет заменён на массовый список кортежей через execute_values.
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
            # Выполняем SELECT-запрос, получаем строки для вставки
            cur.execute(select_query)
            rows = cur.fetchall()
            if rows:
                # Преобразуем каждую строку в кортеж из 6 элементов
                data_tuples = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
                # Выполняем массовую вставку/обновление (пакет по 1000 записей)
                execute_values(cur, insert_query, data_tuples, page_size=1000)
                conn.commit()   # Фиксируем транзакцию
                print(f"Витрина обновлена. Добавлено/обновлено записей: {len(data_tuples)}")
            else:
                print("Нет данных для вставки.")

        print("Витрина dmr.analytics_student успешно создана/обновлена.")

    except Exception as e:
        # При любой ошибке выводим сообщение и откатываем транзакцию (если соединение открыто)
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
        raise   # Пробрасываем исключение дальше, чтобы Airflow узнал о неудаче
    finally:
        # В любом случае закрываем соединение с БД
        if conn:
            conn.close()

# Если скрипт запущен напрямую (а не импортирован как модуль), вызываем create_mart()
if __name__ == "__main__":
    create_mart()