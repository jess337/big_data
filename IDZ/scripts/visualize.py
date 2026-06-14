# Импорт модулей для работы с окружением, данными, визуализацией и БД
import os                   # чтение переменных окружения (хост, порт, логин, пароль)
import json                 # преобразование JSON-строки monthly_trend в Python-объект
import pandas as pd         # загрузка данных из БД в DataFrame
import matplotlib.pyplot as plt   # построение графиков
import seaborn as sns       # улучшение стиля графиков (необязательно, но импортировано)
import psycopg2             # подключение к PostgreSQL

def get_db_connection():
    """
    Создаёт и возвращает соединение с базой данных PostgreSQL.
    Параметры подключения (хост, порт, имя БД, пользователь, пароль)
    берутся из переменных окружения, переданных в контейнер Airflow.
    """
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def visualize():
    """
    Генерирует три графика на основе данных из витрин и сохраняет их в папку /opt/airflow/data.
    1. Круговая диаграмма топ-10 диагнозов.
    2. Линейный график количества приёмов по дням.
    3. Линейный график помесячной динамики заболеваемости для трёх самых частых диагнозов.
    """
    # Подключение к базе данных
    conn = get_db_connection()

    # ------------------ 1. Круговая диаграмма (топ-10 диагнозов) ------------------
    # Загружаем 10 самых частых диагнозов из витрины mart.disease_stats
    df_diag = pd.read_sql("SELECT diagnosis_code, count FROM mart.disease_stats ORDER BY count DESC LIMIT 10", conn)
    # Создаём фигуру размером 10x8 дюймов
    plt.figure(figsize=(10, 8))
    # Строим круговую диаграмму: сектора – количество случаев, подписи – коды диагнозов,
    # autopct='%1.1f%%' показывает процентную долю каждого сектора
    plt.pie(df_diag['count'], labels=df_diag['diagnosis_code'], autopct='%1.1f%%')
    plt.title('Топ‑10 диагнозов')                 # заголовок
    plt.savefig('/opt/airflow/data/diagnosis_pie.png')  # сохраняем в файл
    plt.close()                                   # закрываем фигуру, освобождаем память

    # ------------------ 2. Линейный график количества приёмов по дням ------------------
    # Загружаем ежедневное число приёмов из сырой таблицы raw.appointments
    df_daily = pd.read_sql("""
        SELECT DATE(datetime) AS day, COUNT(*) AS cnt
        FROM raw.appointments
        GROUP BY day ORDER BY day
    """, conn)
    # Создаём фигуру 12x6 дюймов
    plt.figure(figsize=(12, 6))
    # Рисуем линию с маркерами-кружками в каждой точке
    plt.plot(df_daily['day'], df_daily['cnt'], marker='o', linestyle='-')
    plt.title('Динамика количества приёмов по дням')
    plt.xlabel('Дата')
    plt.ylabel('Количество приёмов')
    plt.xticks(rotation=45)      # поворачиваем подписи дат для удобства чтения
    plt.tight_layout()           # автоматически подгоняет поля
    plt.savefig('/opt/airflow/data/daily_appointments.png')
    plt.close()

    # ------------------ 3. Помесячная динамика заболеваемости (топ-3 диагноза) ------------------
    # Загружаем коды диагнозов и их monthly_trend (JSON-массив) из витрины disease_stats
    df_trend = pd.read_sql("SELECT diagnosis_code, monthly_trend FROM mart.disease_stats", conn)
    # Создаём фигуру 12x6 дюймов
    plt.figure(figsize=(12, 6))
    # Ограничиваемся первыми тремя строками (топ-3 диагноза по умолчанию не отсортированы,
    # можно было бы добавить ORDER BY count DESC, но для демонстрации берём head(3))
    for idx, row in df_trend.head(3).iterrows():
        trend = row['monthly_trend']
        if trend:   # если JSON-массив не пустой
            # Если значение из БД – строка, преобразуем её в список словарей через json.loads
            data = json.loads(trend) if isinstance(trend, str) else trend
            months = [d['month'] for d in data]    # извлекаем месяцы
            counts = [d['count'] for d in data]    # извлекаем количества
            plt.plot(months, counts, marker='.', label=row['diagnosis_code'])  # строим линию
    plt.title('Помесячная динамика заболеваемости (топ‑3 диагноза)')
    plt.xlabel('Месяц')
    plt.ylabel('Количество случаев')
    plt.legend()                      # отображаем легенду (названия диагнозов)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/opt/airflow/data/monthly_trends.png')
    plt.close()

    # Закрываем соединение с базой данных
    conn.close()
    print("Графики сохранены в /opt/airflow/data/")

# Если скрипт запускается напрямую (не импортируется как модуль), выполняем функцию visualize()
if __name__ == "__main__":
    visualize()