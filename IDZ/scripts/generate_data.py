# Импорт библиотек для работы с данными, генерации случайных значений и файловой системы
import pandas as pd                     # для создания DataFrame и сохранения в CSV
import random                           # для случайного выбора диагнозов, жалоб, лекарств и т.д.
import json                             # для сериализации назначений в JSON-строку
from faker import Faker                 # для генерации реалистичных ФИО, дат, адресов, телефонов
from datetime import datetime, timedelta # для работы с датами приёма и больничных
import os                               # для создания папок и путей

# Инициализация генератора Faker с русской локализацией
fake = Faker('ru_RU')
# Фиксация seed для воспроизводимости результатов (одинаковые данные при каждом запуске)
Faker.seed(42)
random.seed(42)

# ========================== 1. Генерация пациентов ==========================
def generate_patients(n: int) -> pd.DataFrame:
    """
    Создаёт DataFrame с n записями пациентов.
    """
    patients = []   # список для накопления словарей с данными пациентов
    for i in range(1, n + 1):
        # Генерация персональных данных с помощью Faker
        last_name = fake.last_name()
        first_name = fake.first_name()
        patronymic = fake.middle_name()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90)   # возраст от 18 до 90 лет
        gender = random.choice(['М', 'Ж'])                                 # случайный пол
        insurance_policy = fake.bothify(text='##############')             # 14 цифр (номер полиса)
        snils = fake.bothify(text='###-###-### ##')                        # формат СНИЛС
        address = fake.address().replace('\n', ', ')                       # адрес без переносов строк
        phone = fake.phone_number()                                        # номер телефона

        patients.append({
            'patient_id': i,
            'last_name': last_name,
            'first_name': first_name,
            'patronymic': patronymic,
            'birth_date': birth_date,
            'gender': gender,
            'insurance_policy': insurance_policy,
            'snils': snils,
            'address': address,
            'phone': phone
        })
    return pd.DataFrame(patients)   # преобразуем список словарей в DataFrame

# ========================== 2. Генерация врачей ==========================
def generate_doctors(n: int) -> pd.DataFrame:
    """
    Создаёт DataFrame с n записями врачей.
    """
    specialties = ['Терапевт', 'Хирург', 'Офтальмолог', 'ЛОР', 'Невролог', 
                   'Кардиолог', 'Дерматолог', 'Педиатр', 'Психиатр', 'Эндокринолог']
    doctors = []
    for i in range(1, n + 1):
        last_name = fake.last_name()
        first_name = fake.first_name()
        patronymic = fake.middle_name()
        specialty = random.choice(specialties)      # случайная специальность из списка
        office = random.randint(100, 500)           # номер кабинета (100–500)
        doctors.append({
            'doctor_id': i,
            'last_name': last_name,
            'first_name': first_name,
            'patronymic': patronymic,
            'specialty': specialty,
            'office': office
        })
    return pd.DataFrame(doctors)

# ========================== 3. Генерация приёмов ==========================
def generate_appointments(patients_df: pd.DataFrame, doctors_df: pd.DataFrame, 
                          min_per_patient=5, max_per_patient=10) -> pd.DataFrame:
    """
    Создаёт записи о приёмах (визитах) для каждого пациента.
    Каждому пациенту назначается случайное количество визитов (от min_per_patient до max_per_patient)
    в случайные даты за последние два года. Для каждого визита выбирается случайный врач,
    диагноз, жалобы и список назначений (лекарства с дозировкой).
    """
    appointments = []
    app_id = 1
    start_date = datetime.now() - timedelta(days=2*365)   # начало периода – два года назад
    end_date = datetime.now()                              # конечная дата – сегодня

    # Справочник диагнозов (код МКБ-10 + описание)
    diagnoses = {
        'J06.9': 'Острая респираторная инфекция верхних дыхательных путей',
        'I10': 'Эссенциальная гипертензия',
        'E11': 'Сахарный диабет 2 типа',
        'M54.5': 'Боль в пояснице',
        'H25': 'Старческая катаракта',
        'J45': 'Астма',
        'K29.7': 'Гастрит',
        'N20': 'Мочекаменная болезнь'
    }
    complaints_list = [
        'Кашель, насморк, температура',
        'Головная боль, головокружение',
        'Боли в пояснице, ограничение движений',
        'Одышка, слабость',
        'Боль в горле, осиплость голоса',
        'Снижение зрения',
        'Боли в животе, тошнота',
        'Частое мочеиспускание'
    ]
    medications = [   # список базовых названий лекарств
        'Амоксициллин', 'Парацетамол', 'Ибупрофен', 'Но-шпа', 'Лозартан',
        'Метформин', 'Аторвастатин', 'Омепразол', 'Диклофенак', 'Цетрин',
        'Амлодипин', 'Преднизолон', 'Дексаметазон', 'Флуоксетин', 'Азитромицин'
    ]

    # Для каждого пациента генерируем визиты
    for _, patient in patients_df.iterrows():
        patient_id = patient['patient_id']
        num_appointments = random.randint(min_per_patient, max_per_patient)   # число визитов
        # Генерация дат и сортировка по возрастанию
        dates = sorted([fake.date_between(start_date=start_date, end_date=end_date) 
                        for _ in range(num_appointments)])
        for visit_date in dates:
            doctor = doctors_df.sample(1).iloc[0]                # случайный врач
            diagnosis_code = random.choice(list(diagnoses.keys())) # случайный диагноз
            complaints = random.choice(complaints_list)           # случайная жалоба

            # Генерация назначений (лекарств)
            prescriptions = []
            num_drugs = random.randint(0, 3)          # от 0 до 3 назначений
            for _ in range(num_drugs):
                drug = random.choice(medications) + " " + str(random.randint(100, 500)) + "мг"
                dosage = f"{random.randint(1,3)} раз(а) в день по {random.randint(1,2)} таб."
                duration = f"{random.randint(5,14)} дней"
                prescriptions.append({
                    'medication': drug,
                    'dosage': dosage,
                    'course': duration
                })

            # Формируем запись о приёме
            appointments.append({
                'appointment_id': app_id,
                'datetime': datetime.combine(visit_date, fake.time_object()),   # дата + случайное время
                'patient_id': patient_id,
                'doctor_id': doctor['doctor_id'],
                'diagnosis_code': diagnosis_code,
                'diagnosis_name': diagnoses[diagnosis_code],
                'complaints': complaints,
                'prescriptions': json.dumps(prescriptions, ensure_ascii=False)   # список препаратов в JSON
            })
            app_id += 1
    return pd.DataFrame(appointments)

# ========================== 4. Генерация больничных листов ==========================
def generate_sick_leaves(appointments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Создаёт записи о больничных листах.
    Примерно 30% приёмов сопровождаются выдачей больничного.
    Длительность больничного – от 3 до 15 дней.
    """
    sick_leaves = []
    sl_id = 1
    for _, app in appointments_df.iterrows():
        if random.random() < 0.3:                     # 30% вероятность
            start_date = app['datetime'].date()
            duration = random.randint(3, 15)          # длительность в днях
            end_date = start_date + timedelta(days=duration)
            sick_leaves.append({
                'sick_leave_id': sl_id,
                'appointment_id': app['appointment_id'],
                'start_date': start_date,
                'end_date': end_date,
                'diagnosis': app['diagnosis_name']
            })
            sl_id += 1
    return pd.DataFrame(sick_leaves)

# ========================== 5. Главная функция генерации всех данных ==========================
def generate_all(output_dir="/opt/airflow/data"):
    """
    Последовательно генерирует пациентов, врачей, приёмы и больничные,
    сохраняет каждый DataFrame в отдельный CSV-файл в указанной директории.
    """
    os.makedirs(output_dir, exist_ok=True)            # создаём папку, если её нет
    print("Генерация пациентов...")
    patients = generate_patients(2000)                # 2000 пациентов
    patients.to_csv(os.path.join(output_dir, "patients.csv"), index=False)

    print("Генерация врачей...")
    doctors = generate_doctors(50)                   # 50 врачей
    doctors.to_csv(os.path.join(output_dir, "doctors.csv"), index=False)

    print("Генерация приёмов...")
    appointments = generate_appointments(patients, doctors)   # связь пациентов и врачей
    appointments.to_csv(os.path.join(output_dir, "appointments.csv"), index=False)

    print("Генерация больничных...")
    sick_leaves = generate_sick_leaves(appointments)          # на основе приёмов
    sick_leaves.to_csv(os.path.join(output_dir, "sick_leaves.csv"), index=False)

    print("CSV файлы сохранены в", output_dir)

# Если скрипт запускается напрямую (не импортируется как модуль), выполняем генерацию
if __name__ == "__main__":
    generate_all()