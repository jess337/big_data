# Импорт основного класса DAG из Airflow – он определяет рабочий процесс.
from airflow import DAG
# Импорт оператора для выполнения произвольной функции Python внутри задачи.
from airflow.operators.python import PythonOperator
# Импорт для работы с датой и временем (используется в настройках расписания и задержек).
from datetime import datetime, timedelta

# Импорт пользовательских функций из скриптов, расположенных в папке 'scripts'.
# Эти функции выполняют этапы обработки данных:
from generate_data import generate_all   # генерация синтетических CSV-файлов
from load_raw import load_raw            # загрузка CSV в схему raw БД
from transform import transform          # агрегация данных в схему staging
from create_mart import create_mart      # создание витрин (mart) на основе агрегатов
from visualize import visualize          # построение графиков по данным витрин

# Аргументы по умолчанию для всех задач DAG.
default_args = {
    'owner': 'Korchagina',              # владелец DAG (обычно имя или команда)
    'depends_on_past': False,           # задачи не зависят от успеха предыдущих запусков
    'start_date': datetime(2026, 1, 1), # дата первого возможного запуска (для расписания)
    'email_on_failure': False,          # не отправлять email при ошибке
    'email_on_retry': False,            # не отправлять email при повторе
    'retries': 1,                       # количество попыток перезапуска при ошибке
    'retry_delay': timedelta(minutes=5),# задержка между попытками
}

# Определение DAG (Directed Acyclic Graph) – направленного ациклического графа задач.
with DAG(
    'medical_mart_pipeline',            # уникальное имя DAG, отображается в веб-интерфейсе
    default_args=default_args,          # применяем настройки по умолчанию
    description='Генерация данных, загрузка, трансформация, создание витрин и визуализация',
    schedule_interval='0 4 * * *',      # cron-выражение: запуск каждый день в 4:00
    catchup=False,                      # не выполнять пропущенные запуски (только новые)
    tags=['medical', 'mart'],           # метки для группировки DAG в интерфейсе
) as dag:

    # Задача 1: генерация тестовых данных (CSV)
    task_generate = PythonOperator(
        task_id='generate_data',        # идентификатор задачи (должен быть уникальным внутри DAG)
        python_callable=generate_all,   # функция, которая будет выполнена
    )
    # Задача 2: загрузка CSV в схему raw базы данных
    task_load = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw,
    )
    # Задача 3: трансформация данных – создание агрегатов в схеме staging
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )
    # Задача 4: формирование витрин данных (mart.doctor_workload, mart.disease_stats)
    task_mart = PythonOperator(
        task_id='create_mart',
        python_callable=create_mart,
    )
    # Задача 5: визуализация – построение графиков по витринам
    task_viz = PythonOperator(
        task_id='visualize',
        python_callable=visualize,
    )

    # Определение последовательности выполнения: задачи запускаются строго одна за другой.
    # Оператор >> задаёт порядок: сначала generate_data, затем load_raw, потом transform_data,
    # затем create_mart и в конце visualize.
    task_generate >> task_load >> task_transform >> task_mart >> task_viz