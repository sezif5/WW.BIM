# -*- coding: utf-8 -*-
"""
NWC Export Timer — таймер для ежедневного экспорта.
Дёргает ExternalEvent при совпадении времени.
"""

import datetime
from pyrevit import events, forms

# =====================
# НАСТРОЙКИ
# =====================
EXPORT_TIME = "00:00"  # Формат HH:MM
CHECK_INTERVAL_MINUTES = 5  # Проверять каждые 5 минут
EXTERNAL_EVENT_NAME = "daily_nwc_export"  # Имя ExternalEvent

# Глобальная переменная для флага ежедневного режима
DAILY_EXPORT_MODE = False


class NWCExportTimer:
    """Таймер для ежедневного экспорта"""

    def __init__(self):
        self.timer = None
        self.last_export_date = None

    def start(self):
        """Запуск таймера"""
        if self.timer is not None:
            print("[Timer] Timer уже запущен")
            return

        from System.Timers import Timer
        from System.Threading import TimerCallback

        self.timer = Timer(
            CHECK_INTERVAL_MINUTES * 60 * 1000,
            TimerCallback(self.check_and_export),
            None,
            False,
            False,
        )
        self.timer.Start()
        print("[Timer] Таймер запущен. Время экспорта: {}".format(EXPORT_TIME))
        print("[Timer] Интервал проверки: {} минут".format(CHECK_INTERVAL_MINUTES))

    def stop(self):
        """Остановка таймера"""
        if self.timer:
            self.timer.Stop()
            self.timer.Dispose()
            self.timer = None
            print("[Timer] Таймер остановлен")

    def check_and_export(self):
        """
        Проверка времени и дергание ExternalEvent.

        Логика:
        1. Проверяет текущее время
        2. Сравнивает с временем экспорта
        3. При совпадении → дёргает ExternalEvent 'daily_nwc_export'
        4. Защищает от повторного запуска (один раз в день)
        """
        global DAILY_EXPORT_MODE

        now = datetime.datetime.now()
        today_date = now.date()

        print("[Timer] Проверка времени: {}".format(now.strftime("%H:%M:%S")))

        # Защита от повторного запуска в один день
        if self.last_export_date == today_date:
            print("[Timer] Уже экспортировано сегодня")
            return

        # Формируем целевое время
        try:
            target_time = datetime.datetime.strptime(
                now.strftime("%Y-%m-%d ") + EXPORT_TIME, "%Y-%m-%d %H:%M"
            )
        except ValueError:
            forms.alert(
                "Неверный формат времени экспорта: {}".format(EXPORT_TIME),
                ok=True,
                exitscript=False,
            )
            return

        # Проверяем, совпадает ли время (с допуском 5 минут)
        time_diff = abs((now - target_time).total_seconds())

        if time_diff < 300:  # 5 минут (300 секунд)
            print("[Timer] Время совпало! Запускаем экспорт...")

            # Дёргаем ExternalEvent
            print("[Timer] ExternalEvent: {}".format(EXTERNAL_EVENT_NAME))
            events.send_external_event(EXTERNAL_EVENT_NAME, "")

            # Запоминаем дату экспорта
            self.last_export_date = today_date

            # Защита от повторного запуска
            DAILY_EXPORT_MODE = True
        else:
            print("[Timer] Ожидание... (diff: {:.0f} минут)".format(time_diff / 60))


# =====================
# ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР ТАЙМЕРА
# =====================
nwct_timer = NWCExportTimer()


# =====================
# ИНИЦИАЛИЗАЦИЯ ПРИ ЗАГРУЗКЕ
# =====================
def startup():
    """Инициализация таймера при загрузке pyRevit"""
    nwct_timer.start()

    print("========================================")
    print("NWC Export Timer")
    print("========================================")
    print("Запущен таймер для ежедневного экспорта")
    print("Время экспорта: {}".format(EXPORT_TIME))
    print("Интервал проверки: {} минут".format(CHECK_INTERVAL_MINUTES))
    print("ExternalEvent: {}".format(EXTERNAL_EVENT_NAME))
    print("========================================")


# Вызываем функцию инициализации
startup()
