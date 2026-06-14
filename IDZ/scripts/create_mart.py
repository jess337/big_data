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

def create_mart():
    """
    Создаёт итоговые витрины данных в схеме mart на основе агрегированных данных из схемы staging.
    - Создаёт схему mart, если она не существует.
    - Формирует таблицу mart.doctor_workload (нагрузка врачей и топ‑диагнозы).
    - Формирует таблицу mart.disease_stats (статистика по диагнозам).
    - Добавляет первичные ключи для обеих таблиц.
    """
    # 1. Подключаемся к базе данных
    conn = get_db_connection()
    conn.autocommit = False          # управление транзакциями вручную
    cur = conn.cursor()

    # 2. Создаём схему mart (если её нет)
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart;")
    conn.commit()

    # 3. Витрина doctor_workload – загружает данные из staging.doctor_agg
    #    (doctor_id, specialty, total_appointments, top_diagnoses)
    cur.execute("""
        DROP TABLE IF EXISTS mart.doctor_workload;      -- удаляем старую версию, если есть
        CREATE TABLE mart.doctor_workload AS
        SELECT 
            doctor_id,
            specialty,
            total_appointments,
            top_diagnoses
        FROM staging.doctor_agg;
    """)

    # 4. Витрина disease_stats – загружает данные из staging.disease_agg
    #    (diagnosis_code, count, avg_age, gender_ratio, monthly_trend)
    cur.execute("""
        DROP TABLE IF EXISTS mart.disease_stats;        -- удаляем старую версию, если есть
        CREATE TABLE mart.disease_stats AS
        SELECT 
            diagnosis_code,
            count,
            avg_age,
            gender_ratio,
            monthly_trend
        FROM staging.disease_agg;
    """)

    # 5. Добавляем первичные ключи для обеспечения уникальности записей
    cur.execute("ALTER TABLE mart.doctor_workload ADD PRIMARY KEY (doctor_id);")
    cur.execute("ALTER TABLE mart.disease_stats ADD PRIMARY KEY (diagnosis_code);")

    # 6. Фиксируем изменения и закрываем соединение
    conn.commit()
    cur.close()
    conn.close()
    print("Витрины mart.doctor_workload и mart.disease_stats успешно созданы.")

# Если скрипт запускается напрямую (не импортируется как модуль), выполняем создание витрин
if __name__ == "__main__":
    create_mart()