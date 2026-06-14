# Импорт модулей для работы с окружением, данными и PostgreSQL
import os                   # для чтения переменных окружения (пароль, хост, порт)
import pandas as pd         # для чтения CSV-файлов и работы с DataFrame
import psycopg2             # для подключения к PostgreSQL и выполнения SQL-запросов

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

def load_raw():
    """
    Загружает данные из CSV-файлов (сгенерированных на предыдущем шаге) в схему raw.
    - Создаёт схему raw, если её нет.
    - Создаёт таблицы patients, doctors, appointments, sick_leaves.
    - Читает CSV-файлы и вставляет строки в соответствующие таблицы.
    - Использует ON CONFLICT DO NOTHING, чтобы избежать дублирования при повторных запусках.
    """
    # 1. Подключаемся к БД
    conn = get_db_connection()
    # Отключаем автоматический коммит, чтобы контролировать транзакции вручную
    conn.autocommit = False
    cur = conn.cursor()

    # 2. Создаём схему raw (если не существует)
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    conn.commit()               # фиксируем создание схемы

    # 3. Создаём таблицы в схеме raw
    cur.execute("""
        -- Таблица пациентов
        CREATE TABLE IF NOT EXISTS raw.patients (
            patient_id INTEGER PRIMARY KEY,
            last_name TEXT, first_name TEXT, patronymic TEXT,
            birth_date DATE, gender TEXT, insurance_policy TEXT,
            snils TEXT, address TEXT, phone TEXT
        );
        -- Таблица врачей
        CREATE TABLE IF NOT EXISTS raw.doctors (
            doctor_id INTEGER PRIMARY KEY,
            last_name TEXT, first_name TEXT, patronymic TEXT,
            specialty TEXT, office INTEGER
        );
        -- Таблица приёмов (JSONB для назначений)
        CREATE TABLE IF NOT EXISTS raw.appointments (
            appointment_id INTEGER PRIMARY KEY,
            datetime TIMESTAMP, patient_id INTEGER, doctor_id INTEGER,
            diagnosis_code TEXT, diagnosis_name TEXT,
            complaints TEXT, prescriptions JSONB
        );
        -- Таблица больничных листов
        CREATE TABLE IF NOT EXISTS raw.sick_leaves (
            sick_leave_id INTEGER PRIMARY KEY,
            appointment_id INTEGER, start_date DATE, end_date DATE,
            diagnosis TEXT
        );
    """)
    conn.commit()

    # 4. Путь к папке с CSV-файлами (монтируется из хоста в /opt/airflow/data)
    data_dir = "/opt/airflow/data"

    # 5. Загрузка пациентов
    patients_df = pd.read_csv(os.path.join(data_dir, "patients.csv"))
    for _, row in patients_df.iterrows():
        # Вставка строки: 10 полей – 10 плейсхолдеров %s
        cur.execute("""
            INSERT INTO raw.patients VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (patient_id) DO NOTHING   -- если пациент уже есть, пропускаем
        """, (row['patient_id'], row['last_name'], row['first_name'], row['patronymic'],
              row['birth_date'], row['gender'], row['insurance_policy'],
              row['snils'], row['address'], row['phone']))

    # 6. Загрузка врачей
    doctors_df = pd.read_csv(os.path.join(data_dir, "doctors.csv"))
    for _, row in doctors_df.iterrows():
        cur.execute("""
            INSERT INTO raw.doctors VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (doctor_id) DO NOTHING
        """, (row['doctor_id'], row['last_name'], row['first_name'], row['patronymic'],
              row['specialty'], row['office']))

    # 7. Загрузка приёмов (поле prescriptions – JSONB, нужно преобразовать строку в JSON)
    app_df = pd.read_csv(os.path.join(data_dir, "appointments.csv"))
    for _, row in app_df.iterrows():
        # Если prescriptions пустое или NaN, подставляем пустой массив JSON
        prescriptions_json = row['prescriptions'] if pd.notna(row['prescriptions']) else '[]'
        cur.execute("""
            INSERT INTO raw.appointments (appointment_id, datetime, patient_id, doctor_id,
                diagnosis_code, diagnosis_name, complaints, prescriptions)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (appointment_id) DO NOTHING
        """, (row['appointment_id'], row['datetime'], row['patient_id'], row['doctor_id'],
              row['diagnosis_code'], row['diagnosis_name'], row['complaints'],
              prescriptions_json))

    # 8. Загрузка больничных листов
    sl_df = pd.read_csv(os.path.join(data_dir, "sick_leaves.csv"))
    for _, row in sl_df.iterrows():
        cur.execute("""
            INSERT INTO raw.sick_leaves VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (sick_leave_id) DO NOTHING
        """, (row['sick_leave_id'], row['appointment_id'], row['start_date'],
              row['end_date'], row['diagnosis']))

    # 9. Фиксируем все изменения и закрываем соединение
    conn.commit()
    cur.close()
    conn.close()
    print("Raw данные загружены в БД.")

# Если скрипт запускается напрямую (не импортируется как модуль), выполняем загрузку
if __name__ == "__main__":
    load_raw()