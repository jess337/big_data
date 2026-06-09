# dags/dag_analytics_student_performance.py
# Данный DAG (Directed Acyclic Graph) предназначен для автоматического создания и обновления
# расширенной аналитической витрины dmr.analytics_student_performance в базе данных.
# Он запускается по расписанию или вручную через веб-интерфейс Airflow.

# Импортируем класс DAG — основной контейнер для описания набора задач
from airflow import DAG
# Импортируем оператор PythonOperator, который позволяет выполнять произвольную Python-функцию
from airflow.operators.python import PythonOperator
# Импортируем datetime и timedelta для работы с датами и временем (расписание, задержки)
from datetime import datetime, timedelta
# Импортируем sys для добавления пути к папке со скриптами
import sys
# Импортируем os для работы с путями к файлам
import os

# Добавляем путь к папке 'scripts', которая находится внутри папки 'dags',
# чтобы Python мог импортировать наш скрипт построения витрины.
# os.path.dirname(__file__) возвращает путь к текущему файлу (dags/),
# затем присоединяем 'scripts' — получаем полный путь к папке со скриптами.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

# Импортируем функцию create_mart_performance из нашего скрипта build_mart_performance.py.
# Эта функция содержит всю логику: подключение к БД, создание схемы, таблицы,
# агрегацию данных и заполнение витрины.
from build_mart_performance import create_mart_performance

# Словарь default_args содержит параметры по умолчанию для всех задач DAG.
# Они будут применены к каждой задаче, если не переопределены отдельно.
default_args = {
    'owner': 'Korchagina',                 # Владелец DAG 
    'depends_on_past': False,              # Зависимость от предыдущего запуска: False — задачи не ждут успеха прошлого запуска
    'start_date': datetime(2026, 1, 1),    # Дата, с которой DAG начинает планироваться 
    'email_on_failure': False,             # Не отправлять email при ошибке выполнения задачи
    'email_on_retry': False,               # Не отправлять email при повторной попытке
    'retries': 1,                          # Количество повторных попыток при падении задачи
    'retry_delay': timedelta(minutes=5),   # Задержка между повторными попытками 
}

# Блок with создаёт контекст DAG с уникальным идентификатором 'create_analytics_mart_performance'
with DAG(
    'create_analytics_mart_performance',                # Уникальное имя DAG в системе Airflow
    default_args=default_args,                          # Применяем настройки по умолчанию
    description='Создание и обновление витрины dmr.analytics_student_performance',  # Описание 
    schedule_interval='0 3 * * *',                      # Расписание в формате cron: каждый день в 3:00 UTC
    catchup=False,                                      # Не выполнять пропущенные запуски 
    tags=['mart', 'student_performance'],               # Теги для фильтрации в интерфейсе Airflow
) as dag:

    # Определяем задачу (task) типа PythonOperator.
    # При выполнении DAG эта задача будет запускать функцию create_mart_performance.
    create_mart_task = PythonOperator(
        task_id='create_performance_mart',              # Уникальный ID задачи внутри DAG
        python_callable=create_mart_performance,        # Функция, которая будет вызвана
    )

    # Указываем порядок выполнения задач (в данном случае всего одна задача).
    create_mart_task