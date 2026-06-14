# Импорт модулей для работы с окружением и PostgreSQL
import os                   # для чтения переменных окружения (хост, порт, пароль)
import psycopg2             # для подключения к БД и выполнения SQL-запросов

def get_db_connection():
    """
    Устанавливает соединение с базой данных PostgreSQL.
    Параметры подключения берутся из переменных окружения (DB_HOST, DB_PORT и т.д.),
    которые задаются в .env и передаются в контейнер Airflow.
    """
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),          # хост БД (например, host.docker.internal)
        port=os.getenv('DB_PORT'),          # порт БД (5433 для вашей целевой БД)
        database=os.getenv('DB_NAME'),      # имя базы (my_db_Korchagina)
        user=os.getenv('DB_USER'),          # пользователь (Korchagina)
        password=os.getenv('DB_PASSWORD')   # пароль (из .env)
    )

def transform():
    """
    Выполняет агрегацию данных из схемы raw и сохраняет результаты в схему staging.
    - Создаёт схему staging, если её нет.
    - Формирует таблицу staging.doctor_agg: для каждого врача общее число приёмов
      и топ‑5 диагнозов (в JSON).
    - Формирует таблицу staging.disease_agg: для каждого диагноза общее число случаев,
      средний возраст пациентов, долю мужчин (gender_ratio) и помесячный тренд (JSON).
    """
    # 1. Подключаемся к базе данных
    conn = get_db_connection()
    conn.autocommit = False          # управление транзакциями вручную
    cur = conn.cursor()

    # 2. Создаём схему staging, если она не существует
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()

    # ==================== 3. Агрегация по врачам ====================
    cur.execute("""
        -- Удаляем предыдущую версию таблицы, если есть (CASCADE удаляет зависимые объекты)
        DROP TABLE IF EXISTS staging.doctor_agg CASCADE;
        -- Создаём новую таблицу на основе запроса
        CREATE TABLE staging.doctor_agg AS
        -- 3.1 Подсчёт числа приёмов по каждому врачу и диагнозу
        WITH doctor_diagnosis_counts AS (
            SELECT 
                d.doctor_id,
                d.specialty,
                a.diagnosis_code,
                COUNT(*) AS cnt                     -- количество приёмов с данным диагнозом у врача
            FROM raw.doctors d
            JOIN raw.appointments a ON d.doctor_id = a.doctor_id
            GROUP BY d.doctor_id, d.specialty, a.diagnosis_code
        ),
        -- 3.2 Ранжирование диагнозов для каждого вража по убыванию числа приёмов
        ranked AS (
            SELECT 
                doctor_id,
                specialty,
                diagnosis_code,
                cnt,
                ROW_NUMBER() OVER (PARTITION BY doctor_id ORDER BY cnt DESC) AS rn
            FROM doctor_diagnosis_counts
        )
        -- 3.3 Финальная агрегация: общее число приёмов и топ‑5 диагнозов в JSON
        SELECT 
            doctor_id,
            specialty,
            SUM(cnt) AS total_appointments,        -- общее количество приёмов врача
            JSONB_AGG(
                JSONB_BUILD_OBJECT('diagnosis', diagnosis_code, 'count', cnt)
                ORDER BY cnt DESC
            ) FILTER (WHERE rn <= 5) AS top_diagnoses   -- массив объектов JSON (диагноз → число случаев)
        FROM ranked
        WHERE rn <= 5
        GROUP BY doctor_id, specialty;
    """)
    conn.commit()

    # ==================== 4. Агрегация по диагнозам ====================
    cur.execute("""
        DROP TABLE IF EXISTS staging.disease_agg CASCADE;
        CREATE TABLE staging.disease_agg AS
        -- 4.1 Возраст пациентов (полных лет на текущую дату)
        WITH patient_age AS (
            SELECT 
                p.patient_id,
                EXTRACT(YEAR FROM AGE(p.birth_date)) AS age
            FROM raw.patients p
        ),
        -- 4.2 Помесячное число приёмов по каждому диагнозу
        monthly_counts AS (
            SELECT 
                a.diagnosis_code,
                DATE_TRUNC('month', a.datetime) AS month,   -- первый день месяца
                COUNT(*) AS cnt
            FROM raw.appointments a
            GROUP BY a.diagnosis_code, DATE_TRUNC('month', a.datetime)
        ),
        -- 4.3 Соотношение полов по диагнозам (количество мужчин и женщин)
        gender_ratio AS (
            SELECT 
                a.diagnosis_code,
                COUNT(CASE WHEN p.gender = 'М' THEN 1 END) AS male_cnt,
                COUNT(CASE WHEN p.gender = 'Ж' THEN 1 END) AS female_cnt
            FROM raw.appointments a
            JOIN raw.patients p ON a.patient_id = p.patient_id
            GROUP BY a.diagnosis_code
        )
        -- 4.4 Итоговая таблица: общее число случаев, средний возраст,
        --     доля мужчин (gender_ratio), помесячный тренд (JSON-массив)
        SELECT 
            a.diagnosis_code,
            COUNT(*) AS count,                              -- общее количество приёмов с данным диагнозом
            AVG(pa.age) AS avg_age,                         -- средний возраст пациентов (без округления)
            CASE 
                WHEN (male_cnt + female_cnt) > 0 
                THEN (male_cnt * 1.0 / (male_cnt + female_cnt))   -- доля мужчин (0..1)
                ELSE NULL
            END AS gender_ratio,
            -- Подзапрос: собираем JSON-массив объектов {month, count} для каждого месяца
            (SELECT JSONB_AGG(JSONB_BUILD_OBJECT('month', month, 'count', cnt) ORDER BY month)
             FROM monthly_counts mc WHERE mc.diagnosis_code = a.diagnosis_code) AS monthly_trend
        FROM raw.appointments a
        JOIN patient_age pa ON a.patient_id = pa.patient_id
        JOIN gender_ratio gr ON a.diagnosis_code = gr.diagnosis_code
        GROUP BY a.diagnosis_code, male_cnt, female_cnt;
    """)
    conn.commit()

    # 5. Закрываем курсор и соединение
    cur.close()
    conn.close()
    print("Трансформация завершена, агрегаты сохранены в staging.")

# Если скрипт запускается напрямую (не импортируется как модуль), выполняем трансформацию
if __name__ == "__main__":
    transform()