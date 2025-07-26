import logging
import asyncio
import os
import re
from datetime import datetime, timedelta
import pytz
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nails_bot")
API_TOKEN = os.getenv("YOUR_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

# Инициализация бота
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Конфигурация услуг
service_durations = {
    "💅 Маникюр": 2,
    "✨ Наращивание": 2,
    "👡 Педикюр": 2,
    "🦶 Педикюр полный": 2,
    "💎 Комбо (маникюр + педикюр)": 4
}

# Контактная информация
salon_address = "г. Шарья, ул. Юбилейная, тц Венера, вход сзади в подвальное помещение, кабинет третий с конца по правой стороне."
master_contacts = """
📞 Контакты мастера:
👉 Телефон: +7 (915) 914-65-79
👉 Telegram: @Eev_kor
"""


class Database:
    _pool = None

    @classmethod
    async def get_pool(cls):
        if cls._pool is None:
            cls._pool = await asyncpg.create_pool(
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                min_size=5,
                max_size=20,
                max_inactive_connection_lifetime=300
            )
        return cls._pool

    @classmethod
    async def close(cls):
        if cls._pool:
            await cls._pool.close()
            cls._pool = None


class Form(StatesGroup):
    service = State()
    date = State()
    time = State()
    name = State()
    phone = State()
    cancel_appointment = State()


def get_back_to_menu_keyboard():
    """Создает клавиатуру с кнопкой 'Назад в меню'"""
    builder = ReplyKeyboardBuilder()
    builder.add(types.KeyboardButton(text="🔙 Назад в меню"))
    return builder.as_markup(resize_keyboard=True)


async def init_db():
    """Инициализация структуры базы данных"""
    pool = await Database.get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    client_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS appointments (
                    appointment_id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                    service TEXT NOT NULL,
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    confirmed BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_appointments_date ON appointments(date)
            ''')
            logger.info("База данных инициализирована")
        except Exception as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            raise


async def create_user(user_id: int, username: str, client_name: str, phone: str):
    """Создание/обновление пользователя"""
    pool = await Database.get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute('''
                INSERT INTO users (user_id, username, client_name, phone)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    client_name = EXCLUDED.client_name,
                    phone = EXCLUDED.phone
            ''', user_id, username, client_name, phone)
        except Exception as e:
            logger.error(f"Ошибка сохранения пользователя: {e}")
            raise


async def create_appointment(user_id: int, service: str, date: str, time: str):
    """Создание новой записи"""
    pool = await Database.get_pool()
    async with pool.acquire() as conn:
        try:
            return await conn.fetchval('''
                INSERT INTO appointments (user_id, service, date, time)
                VALUES ($1, $2, $3, $4)
                RETURNING appointment_id
            ''', user_id, service, date, time)
        except Exception as e:
            logger.error(f"Ошибка создания записи: {e}")
            raise


async def get_booked_times(date: str):
    """Получение занятых временных слотов"""
    pool = await Database.get_pool()
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch('''
                SELECT time, service FROM appointments WHERE date = $1
            ''', date)
            return [(record['time'], record['service']) for record in records]
        except Exception as e:
            logger.error(f"Ошибка получения занятых слотов: {e}")
            return []


async def get_appointments():
    """Получение всех записей"""
    pool = await Database.get_pool()
    async with pool.acquire() as conn:
        try:
            return await conn.fetch('''
                SELECT a.appointment_id, a.service, a.date, a.time, 
                       u.client_name, u.phone
                FROM appointments a
                JOIN users u ON a.user_id = u.user_id
                ORDER BY a.date, a.time
            ''')
        except Exception as e:
            logger.error(f"Ошибка получения записей: {e}")
            return []


def generate_dates():
    """Генерация доступных дат"""
    dates = []
    today = datetime.now(pytz.timezone('Europe/Moscow'))
    for i in range(1, 31):
        date = today + timedelta(days=i)
        if date.weekday() < 5:  # Только будние дни
            dates.append(date.strftime('%d.%m.%Y'))
    return dates


async def generate_times(service: str, selected_date: str):
    """Генерация доступного времени"""
    duration = service_durations.get(service, 2)
    booked_slots = await get_booked_times(selected_date)
    available_times = []

    start_hour = 10
    end_hour = 20
    current_time = datetime.strptime(f"{selected_date} {start_hour}:00", "%d.%m.%Y %H:%M")
    end_time = datetime.strptime(f"{selected_date} {end_hour}:00", "%d.%m.%Y %H:%M")

    while current_time + timedelta(hours=duration) <= end_time:
        time_slot = current_time.time().strftime('%H:%M')
        slot_end = (current_time + timedelta(hours=duration)).time()

        conflict = False
        for booked_time, booked_service in booked_slots:
            booked_start = datetime.strptime(booked_time, '%H:%M').time()
            booked_duration = service_durations.get(booked_service, 2)
            booked_end = (datetime.strptime(booked_time, '%H:%M') + timedelta(hours=booked_duration)).time()

            if not (slot_end <= booked_start or current_time.time() >= booked_end):
                conflict = True
                break

        if not conflict:
            available_times.append(time_slot)

        current_time += timedelta(hours=2)

    return available_times


@dp.message(Command("start"))
async def send_welcome(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    try:
        await state.clear()

        builder = ReplyKeyboardBuilder()
        services = list(service_durations.keys())

        # Добавляем кнопки услуг
        for service in services:
            builder.add(types.KeyboardButton(text=service))
        builder.adjust(2)

        # Добавляем админские кнопки если пользователь админ
        if str(message.from_user.id) == ADMIN_ID:
            admin_buttons = [
                types.KeyboardButton(text="📋 Мои записи"),
                types.KeyboardButton(text="❌ Отменить запись")
            ]
            builder.row(*admin_buttons)

        await message.answer(
            "🌸 <b>Добро пожаловать в салон красоты!</b> 🌸\n\n"
            "Выберите услугу из меню ниже:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
    except Exception as e:
        logger.error(f"Ошибка в send_welcome: {e}")
        await message.answer("❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.")


@dp.message(F.text.in_(service_durations.keys()))
async def select_service(message: types.Message, state: FSMContext):
    """Обработчик выбора услуги из меню"""
    try:
        service = message.text
        await state.set_state(Form.service)
        await state.update_data(service=service)

        # Генерируем доступные даты
        dates = generate_dates()

        builder = ReplyKeyboardBuilder()
        for date in dates:
            builder.add(types.KeyboardButton(text=date))
        builder.add(types.KeyboardButton(text="🔙 Назад в меню"))
        builder.adjust(3)

        await message.answer(
            "📅 Выберите удобную дату:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(Form.date)
    except Exception as e:
        logger.error(f"Ошибка в select_service: {e}")
        await message.answer(
            "❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.",
            reply_markup=get_back_to_menu_keyboard()
        )


@dp.message(Form.date)
async def process_date(message: types.Message, state: FSMContext):
    """Обработка выбора даты"""
    try:
        selected_date = message.text

        # Обработка кнопки "Назад"
        if selected_date == "🔙 Назад в меню":
            await state.clear()
            await send_welcome(message, state)
            return

        data = await state.get_data()
        service = data['service']

        # Проверяем формат даты
        try:
            datetime.strptime(selected_date, '%d.%m.%Y')
        except ValueError:
            await message.answer(
                "❌ Неверный формат даты. Пожалуйста, выберите дату из предложенных.",
                reply_markup=get_back_to_menu_keyboard()
            )
            return

        await state.update_data(date=selected_date)

        # Генерируем доступное время для выбранной даты и услуги
        available_times = await generate_times(service, selected_date)

        if not available_times:
            await message.answer(
                "❌ На выбранную дату нет свободных слотов. Пожалуйста, выберите другую дату.",
                reply_markup=get_back_to_menu_keyboard()
            )
            return

        builder = ReplyKeyboardBuilder()
        for time in available_times:
            builder.add(types.KeyboardButton(text=time))
        builder.add(types.KeyboardButton(text="🔙 Назад в меню"))
        builder.adjust(3)

        await message.answer(
            "⏰ Выберите удобное время:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(Form.time)
    except Exception as e:
        logger.error(f"Ошибка в process_date: {e}")
        await message.answer(
            "❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.",
            reply_markup=get_back_to_menu_keyboard()
        )


@dp.message(Form.time)
async def process_time(message: types.Message, state: FSMContext):
    """Обработка выбора времени"""
    try:
        selected_time = message.text

        # Обработка кнопки "Назад"
        if selected_time == "🔙 Назад в меню":
            await state.clear()
            await send_welcome(message, state)
            return

        data = await state.get_data()

        # Проверяем формат времени
        try:
            datetime.strptime(selected_time, '%H:%M')
        except ValueError:
            await message.answer(
                "❌ Неверный формат времени. Пожалуйста, выберите время из предложенных.",
                reply_markup=get_back_to_menu_keyboard()
            )
            return

        await state.update_data(time=selected_time)

        await message.answer(
            "👤 Теперь введите ваше имя:",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Form.name)
    except Exception as e:
        logger.error(f"Ошибка в process_time: {e}")
        await message.answer(
            "❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.",
            reply_markup=get_back_to_menu_keyboard()
        )


@dp.message(Form.name)
async def process_name(message: types.Message, state: FSMContext):
    """Обработка ввода имени"""
    try:
        name = message.text.strip()
        if not name or len(name) < 2:
            await message.answer(
                "❌ Имя должно содержать хотя бы 2 символа. Пожалуйста, введите ваше имя.",
                reply_markup=get_back_to_menu_keyboard()
            )
            return

        await state.update_data(name=name)

        await message.answer(
            "📞 Теперь введите ваш номер телефона (например, 89159086325 или +79159086325):",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Form.phone)
    except Exception as e:
        logger.error(f"Ошибка в process_name: {e}")
        await message.answer(
            "❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.",
            reply_markup=get_back_to_menu_keyboard()
        )

@dp.message(Form.phone)
async def process_phone(message: types.Message, state: FSMContext):
    """Обработка ввода телефона и завершение записи"""
    try:
        phone = message.text.strip()
        # Проверяем формат телефона
        if not re.match(r'^(\+7|8|7)\d{10}$', phone):
            await message.answer(
                "❌ Неверный формат телефона. Пожалуйста, введите номер в формате 79159146579 или +79159146579.",
                reply_markup=get_back_to_menu_keyboard()
            )
            return

        data = await state.get_data()

        # Сохраняем пользователя
        await create_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            client_name=data['name'],
            phone=phone
        )

        # Создаем запись
        appointment_id = await create_appointment(
            user_id=message.from_user.id,
            service=data['service'],
            date=data['date'],
            time=data['time']
        )

        # Формируем сообщение с подтверждением
        confirmation_message = (
            "✅ <b>Запись успешно оформлена!</b>\n\n"
            f"🔹 Услуга: {data['service']}\n"
            f"📅 Дата: {data['date']}\n"
            f"⏰ Время: {data['time']}\n"
            f"👤 Имя: {data['name']}\n"
            f"📞 Телефон: {phone}\n\n"
            f"📍 Адрес салона: {salon_address}\n\n"
            f"{master_contacts}"
        )

        await message.answer(confirmation_message)

        # Уведомляем администратора
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    "🆕 Новая запись!\n\n" + confirmation_message
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора: {e}")

        await state.clear()
        await send_welcome(message, state)
    except Exception as e:
        logger.error(f"Ошибка в process_phone: {e}")
        await message.answer(
            "❌ Произошла ошибка при оформлении записи. Пожалуйста, попробуйте еще раз.",
            reply_markup=get_back_to_menu_keyboard()
        )


@dp.message(F.text == "📋 Мои записи")
async def show_appointments(message: types.Message):
    """Показ всех записей для администратора"""
    if str(message.from_user.id) != ADMIN_ID:
        return

    try:
        appointments = await get_appointments()

        if not appointments:
            await message.answer("📭 На данный момент записей нет.")
            return

        response = ["📅 <b>Список записей:</b>\n\n"]
        for appt in appointments:
            appt_info = (
                f"🆔 ID: {appt['appointment_id']}\n"
                f"👤 Клиент: {appt['client_name']}\n"
                f"📞 Телефон: {appt['phone']}\n"
                f"🔹 Услуга: {appt['service']}\n"
                f"📅 Дата: {appt['date']}\n"
                f"⏰ Время: {appt['time']}\n\n"
            )
            response.append(appt_info)

        await message.answer("\n".join(response))

        # Кнопки для админа
        admin_kb = ReplyKeyboardBuilder()
        admin_kb.add(types.KeyboardButton(text="❌ Отменить запись"))
        admin_kb.add(types.KeyboardButton(text="🔙 Назад в меню"))
        admin_kb.adjust(2)

        await message.answer(
            "Для отмены записи нажмите соответствующую кнопку",
            reply_markup=admin_kb.as_markup(resize_keyboard=True)
        )

    except Exception as e:
        logger.error(f"Ошибка при получении записей: {e}")
        await message.answer(
            "❌ Произошла ошибка при получении записей",
            reply_markup=ReplyKeyboardRemove()
        )


@dp.message(F.text == "❌ Отменить запись")
async def cancel_appointment_start(message: types.Message, state: FSMContext):
    """Начало процесса отмены записи"""
    if str(message.from_user.id) != ADMIN_ID:
        return

    await state.set_state(Form.cancel_appointment)
    await message.answer(
        "Введите ID записи для отмены:",
        reply_markup=get_back_to_menu_keyboard()
    )


@dp.message(Form.cancel_appointment)
async def process_cancel_appointment(message: types.Message, state: FSMContext):
    """Обработка отмены записи"""
    if str(message.from_user.id) != ADMIN_ID:
        return

    try:
        # Обработка кнопки "Назад"
        if message.text == "🔙 Назад в меню":
            await state.clear()
            await send_welcome(message, state)
            return

        appointment_id = int(message.text)
    except ValueError:
        await message.answer("❌ Неверный формат ID. Введите число.")
        return

    pool = await Database.get_pool()
    async with pool.acquire() as conn:
        try:
            # Получаем данные записи
            record = await conn.fetchrow(
                '''SELECT a.*, u.user_id, u.client_name 
                FROM appointments a
                JOIN users u ON a.user_id = u.user_id
                WHERE a.appointment_id = $1''',
                appointment_id
            )

            if not record:
                await message.answer("❌ Запись с таким ID не найдена")
                return

            # Удаляем запись
            await conn.execute(
                'DELETE FROM appointments WHERE appointment_id = $1',
                appointment_id
            )

            # Уведомляем администратора
            await message.answer(
                f"✅ Запись ID {appointment_id} отменена\n"
                f"👤 Клиент: {record['client_name']}\n"
                f"📅 Дата: {record['date']}\n"
                f"⏰ Время: {record['time']}"
            )

            # Уведомляем клиента
            try:
                await bot.send_message(
                    record['user_id'],
                    "❌ Ваша запись была отменена администратором\n\n"
                    f"🔹 Услуга: {record['service']}\n"
                    f"📅 Дата: {record['date']}\n"
                    f"⏰ Время: {record['time']}\n\n"
                    f"{master_contacts}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить клиента: {e}")

        except Exception as e:
            logger.error(f"Ошибка отмены записи: {e}")
            await message.answer("❌ Произошла ошибка при отмене записи")
        finally:
            await state.clear()
            await send_welcome(message, state)


@dp.message(F.text == "🔙 Назад в меню")
async def back_to_menu(message: types.Message, state: FSMContext):
    """Возврат в главное меню из любого состояния"""
    try:
        await state.clear()
        await send_welcome(message, state)
    except Exception as e:
        logger.error(f"Ошибка в back_to_menu: {e}")
        await message.answer(
            "❌ Произошла ошибка при возврате в меню.",
            reply_markup=ReplyKeyboardRemove()
        )


async def send_reminders():
    """Фоновая задача для отправки напоминаний"""
    while True:
        try:
            now = datetime.now(pytz.timezone('Europe/Moscow'))
            tomorrow = now + timedelta(days=1)

            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                # Напоминания за день
                if now.hour == 10:  # Отправляем в 10 утра
                    appointments = await conn.fetch(
                        '''SELECT a.*, u.user_id, u.client_name 
                        FROM appointments a
                        JOIN users u ON a.user_id = u.user_id
                        WHERE a.date = $1''',
                        tomorrow.strftime('%d.%m.%Y')
                    )

                    for appt in appointments:
                        try:
                            await bot.send_message(
                                appt['user_id'],
                                "⏰ Напоминание о записи!\n\n"
                                f"Завтра в {appt['time']} у вас запись:\n"
                                f"🔹 {appt['service']}\n"
                                f"📍 Адрес: {salon_address}\n\n"
                                f"{master_contacts}"
                            )
                        except Exception as e:
                            logger.error(f"Не удалось отправить напоминание: {e}")

                # Напоминания за час
                hour_later = now + timedelta(hours=1)
                appointments = await conn.fetch(
                    '''SELECT a.*, u.user_id, u.client_name 
                    FROM appointments a
                    JOIN users u ON a.user_id = u.user_id
                    WHERE a.date = $1 AND a.time = $2''',
                    now.strftime('%d.%m.%Y'),
                    hour_later.strftime('%H:%M')
                )

                for appt in appointments:
                    try:
                        await bot.send_message(
                            appt['user_id'],
                            "⏳ Через час у вас запись!\n\n"
                            f"🔹 Услуга: {appt['service']}\n"
                            f"⏰ Время: {appt['time']}\n"
                            f"📍 Адрес: {salon_address}"
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить напоминание: {e}")

        except Exception as e:
            logger.error(f"Ошибка в задаче напоминаний: {e}")
        finally:
            await asyncio.sleep(3600)  # Проверка каждый час


async def on_startup():
    print("✅ Бот успешно запущен")
    await bot.delete_webhook(drop_pending_updates=True)


async def on_shutdown():
    print("⏳ Завершение работы...")

    # Закрываем хранилище (если используется)
    if hasattr(dp.storage, 'close'):
        await dp.storage.close()

    # Правильное закрытие сессии
    session = await bot.get_session()
    if isinstance(session, AiohttpSession):
        await session.close()

    print("❌ Бот остановлен")


async def main():
    try:
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)

        print("🔄 Запускаем бота...")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except Exception as ex:
        print(f"🚨 Ошибка: {type(ex).__name__}: {ex}")
    finally:
        # Дополнительная гарантия закрытия сессии
        try:
            session = await bot.get_session()
            if isinstance(session, AiohttpSession):
                await session.close()
        except:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлено пользователем (Ctrl+C)")
    except Exception as e:
        print(f"💥 Критическая ошибка: {type(e).__name__}: {e}")
