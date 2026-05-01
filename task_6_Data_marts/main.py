"""
 Требуется создать отдельную схему dmr  (Data Mart Repository) для аналитических данных и 
 разместить в ней витрину analytics_student_performancee.

 Требования:
- Создать схему dmr если она не существует
- Создать витрину dmr.analytics_student_performance с агрегированными данными.
- Реализация через функции

Структура витрины: 
Поле	- Тип данных	- Описание
student_id	- INTEGER	- ID студента
course_id -	INTEGER	ID - курса
department_id -	INTEGER	- Код кафедры
department_name	 - VARCHAR - Название кафедры
education_level	- VARCHAR	- Уровень образования
education_base - VARCHAR -	Основа обучения
semester	- INTEGER	- Номер семестра
course_year	- INTEGER	- Курс обучения
final_grade -	INTEGER -	Итоговая оценка
total_events -	INTEGER	- Всего событий за семестр
avg_weekly_events	- DECIMAL(10,2)	- Среднее событий в неделю
total_course_views	- INTEGER	- Всего просмотров курса
total_quiz_views	- INTEGER	- Всего просмотров тестов
total_module_views -	INTEGER - Всего просмотров модулей
total_submissions	- INTEGER	- Всего отправленных заданий
peak_activity_week	- INTEGER	- Неделя с максимальной активностью
consistency_score	- DECIMAL(5,2)	- Коэффициент стабильности активности (0-1)
activity_category	- VARCHAR	- Категория активности (низкая/средняя/высокая)
last_update	- TIMESTAMP	- Дата обновления записи

"""


# Ниже представлен пример реализации витрины dmr.analytics_student
# Поле	- Тип данных	- Описание
# student_id	- INTEGER	- ID студента
# course_id -	INTEGER	ID - курса
# department_id -	INTEGER	- Код кафедры
# semester	- INTEGER	- Номер семестра
# course_year	- INTEGER	- Курс обучения
# final_grade -	INTEGER -	Итоговая оценка
# last_update	- TIMESTAMP	- Дата обновления записи

# Импортируем модуль os для работы с переменными окружения и файловой системой
import os
# Импортируем sys для выхода из программы при критической ошибке
import sys
# Импортируем функцию load_dotenv для загрузки переменных из файла .env в окружение
from dotenv import load_dotenv
# Импортируем psycopg2 для подключения к PostgreSQL и выполнения SQL-запросов
import psycopg2
# Импортируем sql из psycopg2 для безопасного формирования SQL-запросов (экранирование)
from psycopg2 import sql
# Импортируем execute_values для массовой вставки/обновления записей в таблицу
from psycopg2.extras import execute_values

# Загружаем переменные из файла .env в переменные окружения процесса
load_dotenv()

def get_db_config():
    """Возвращает словарь с параметрами подключения к БД."""
    # Формируем словарь с настройками подключения, беря значения из переменных окружения
    # Если переменная не задана, используем значение по умолчанию
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),   # адрес хоста БД (по умолчанию localhost)
        'port': os.getenv('DB_PORT', '5432'),        # порт БД (по умолчанию 5432)
        'database': os.getenv('DB', 'educational_portal'),  # имя базы данных
        'user': os.getenv('USER', 'postgres'),       # имя пользователя БД
        'password': os.getenv('PASSWORD', '')        # пароль пользователя
    }
    # Выводим параметры подключения, скрывая пароль для безопасности
    print("Параметры подключения:", {k: v for k, v in config.items() if k != 'password'})
    return config

def get_connection():
    """Устанавливает соединение с PostgreSQL."""
    try:
        # Получаем словарь с параметрами подключения
        config = get_db_config()
        # Устанавливаем соединение, передавая параметры как именованные аргументы
        conn = psycopg2.connect(**config)
        # Отключаем автоматический коммит, чтобы управлять транзакциями вручную
        conn.autocommit = False
        # Возвращаем объект соединения
        return conn
    except Exception as e:
        # Если произошла ошибка, выводим сообщение
        print(f"Ошибка подключения: {e}")
        # Завершаем программу с кодом ошибки 1
        sys.exit(1)

def create_schema(conn):
    """Создаёт схему dmr, если она не существует."""
    # Открываем курсор (контекстный менеджер автоматически закроет его)
    with conn.cursor() as cur:
        # Выполняем SQL-команду: создать схему dmr, если её нет
        cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
        # Фиксируем транзакцию (сохраняем изменения)
        conn.commit()
        # Выводим сообщение об успехе
        print("Схема dmr создана (или уже существует).")

def create_table(conn):
    """Создаёт таблицу витрины dmr.analytics_student_performance."""
    # Многострочная строка с SQL-запросом для создания таблицы
    create_query = """
    CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
        student_id          INTEGER NOT NULL,        -- ID студента, обязательное поле
        course_id           INTEGER NOT NULL,        -- ID курса, обязательное поле
        department_id       INTEGER,                 -- ID кафедры
        department_name     VARCHAR,                 -- название кафедры
        education_level     VARCHAR,                 -- уровень образования (бакалавриат, магистратура и т.д.)
        education_base      VARCHAR,                 -- основа обучения (бюджет/контракт)
        semester            INTEGER,                 -- номер семестра
        course_year         INTEGER,                 -- курс обучения
        final_grade         INTEGER,                 -- итоговая оценка
        total_events        INTEGER,                 -- всего событий за семестр
        avg_weekly_events   DECIMAL(10,2),           -- среднее событий в неделю
        total_course_views  INTEGER,                 -- всего просмотров курса
        total_quiz_views    INTEGER,                 -- всего просмотров тестов
        total_module_views  INTEGER,                 -- всего просмотров модулей
        total_submissions   INTEGER,                 -- всего отправленных заданий
        peak_activity_week  INTEGER,                 -- неделя с максимальной активностью
        consistency_score   DECIMAL(5,2),            -- коэффициент стабильности активности (0-1)
        activity_category   VARCHAR,                 -- категория активности (низкая/средняя/высокая)
        last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- время последнего обновления
        PRIMARY KEY (student_id, course_id)          -- составной первичный ключ
    );
    """
    with conn.cursor() as cur:
        # Выполняем создание таблицы
        cur.execute(create_query)
        # Фиксируем изменения
        conn.commit()
        print("Таблица dmr.analytics_student_performance создана.")

def fill_data_mart(conn):
    """
    Агрегирует данные из public.user_logs и departments,
    вычисляет все метрики и вставляет их в витрину.
    """
    # Многострочная строка с SQL-запросом: CTE, агрегация, вставка с конфликтом
    sql_insert = """
    -- 1. Первое CTE: выбираем все нужные колонки из user_logs,
    --    отфильтровывая строки, где оценка NULL (нет смысла),
    --    и сразу приводим текстовые колонки (leveled, name_osno) к целым числам
    WITH weekly_agg AS (
        SELECT 
            userid,
            courseid,
            num_week,
            depart,
            num_sem,
            kurs,
            namer_level,
            leveled::INTEGER AS leveled_int,    -- преобразуем текст в целое число
            name_osno::INTEGER AS name_osno_int, -- преобразуем текст в целое число
            s_all,
            s_course_viewed,
            s_q_attempt_viewed,
            s_a_course_module_viewed,
            s_a_submission_status_viewed
        FROM public.user_logs
        WHERE namer_level IS NOT NULL
          AND leveled ~ '^[0-9]+$'   -- оставляем только числовые значения (предотвращает ошибки)
          AND name_osno ~ '^[0-9]+$' -- аналогично для name_osno
    ),
    -- 2. Второе CTE: приводим оценку к INTEGER (явное преобразование)
    student_week_stats AS (
        SELECT 
            userid,
            courseid,
            depart,
            num_sem,
            kurs,
            CAST(namer_level AS INTEGER) AS final_grade,
            leveled_int,               -- уже целое число
            name_osno_int,             -- уже целое число
            num_week,
            s_all,
            s_course_viewed,
            s_q_attempt_viewed,
            s_a_course_module_viewed,
            s_a_submission_status_viewed
        FROM weekly_agg
    ),
    -- 3. Третье CTE: агрегируем по студенту и курсу:
    --    суммы, максимумы, средние, пиковая неделя, коэффициент стабильности
    aggregated AS (
        SELECT 
            userid AS student_id,
            courseid AS course_id,
            MAX(depart) AS department_id,
            MAX(num_sem) AS semester,
            MAX(kurs) AS course_year,
            MAX(final_grade) AS final_grade,
            MAX(leveled_int) AS level_ed,        -- целое число уровня образования
            MAX(name_osno_int) AS name_osno,     -- целое число основы обучения
            SUM(s_all) AS total_events,
            AVG(s_all) AS avg_weekly_events,
            SUM(s_course_viewed) AS total_course_views,
            SUM(s_q_attempt_viewed) AS total_quiz_views,
            SUM(s_a_course_module_viewed) AS total_module_views,
            SUM(s_a_submission_status_viewed) AS total_submissions,
            -- неделя с максимальным s_all (если несколько, берём первую)
            (array_agg(num_week ORDER BY s_all DESC))[1] AS peak_activity_week,
            -- коэффициент стабильности: 1 - (stddev / mean), ограничен [0,1]
            1 - LEAST(1, COALESCE(STDDEV(s_all) / NULLIF(AVG(s_all), 0), 0)) AS consistency_score
        FROM student_week_stats
        GROUP BY userid, courseid
    ),
    -- 4. Присоединяем название кафедры и преобразуем level_ed, name_osno в текстовые значения
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
    -- 5. Финальные вычисления: округление, категория активности
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
        WHERE final_grade IN (2,3,4,5)   -- только валидные оценки
    )
    -- 6. Вставка или обновление записей в витрине (UPSERT)
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
    -- Если запись с таким student_id, course_id уже существует, обновляем все поля
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
        # Выполняем весь составной SQL-запрос
        cur.execute(sql_insert)
        # Фиксируем транзакцию
        conn.commit()
        # Выводим количество затронутых строк (вставлено или обновлено)
        print(f"Витрина успешно заполнена. Затронуто строк: {cur.rowcount}")

def main():
    """Главная функция, которая последовательно вызывает создание схемы, таблицы и заполнение."""
    conn = None
    try:
        # Получаем соединение с БД
        conn = get_connection()
        # Создаём схему dmr
        create_schema(conn)
        # Создаём таблицу витрины
        create_table(conn)
        # Заполняем витрину данными
        fill_data_mart(conn)
        print("\nВсе операции выполнены успешно!")
    except Exception as e:
        # При любой ошибке выводим сообщение и откатываем транзакцию, если соединение было открыто
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        # Закрываем соединение с БД, если оно было открыто
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")

# Если скрипт запущен напрямую (не импортирован как модуль), вызываем main
if __name__ == "__main__":
    main()