# -*- coding: utf-8 -*-
# script.py — показать/скрыть Dockable Pane "Family Manager" через pyRevit
from __future__ import print_function

from pyrevit import HOST_APP, forms, script
from pyrevit.coreutils import Guid
from Autodesk.Revit import UI

logger = script.get_logger()

# ДОЛЖЕН совпадать с GUID из startup.py
PANE_GUID = "A7E9F8D3-1234-4567-89AB-CDEF01234567"

uiapp = HOST_APP.uiapp
pane_id = UI.DockablePaneId(Guid(PANE_GUID))

# Получаем экземпляр контрола из startup.py для инициализации
ctrl = None
try:
    import sys
    # Ищем модуль startup в уже загруженных модулях
    if 'startup' in sys.modules:
        startup = sys.modules['startup']
        logger.info("Found startup module in sys.modules")
    else:
        # Если не найден, импортируем (но это не должно случиться)
        import startup
        logger.info("Imported startup module")
    
    if hasattr(startup, '_CTRL_INSTANCE') and startup._CTRL_INSTANCE:
        ctrl = startup._CTRL_INSTANCE
        logger.info("Got control instance from startup")
except Exception as ex:
    logger.warning("Could not get control from startup: {0}".format(ex))

# Пытаемся получить панель
try:
    pane = uiapp.GetDockablePane(pane_id)
    logger.info("Successfully got dockable pane")
except Exception as ex:
    logger.error("Failed to get dockable pane: {0}".format(ex))
    forms.alert("Панель 'Family Manager' не зарегистрирована.\n"
                "Убедитесь, что startup.py отработал (перезапустите Revit).\n\n"
                "Ошибка: {0}".format(ex),
                title="Family Manager", warn_icon=True, exitscript=True)

# Инициализируем панель с текущим UIApplication перед показом
if ctrl and hasattr(ctrl, 'Initialize'):
    try:
        ctrl.Initialize(uiapp)
        logger.info("Family Manager: панель инициализирована с UIApplication.")
    except Exception as ex:
        logger.warning("Не удалось инициализировать панель: {0}".format(ex))

# Переключаем видимость панели
try:
    if pane.IsShown():
        pane.Hide()
        logger.info("Family Manager: панель скрыта.")
    else:
        pane.Show()
        logger.info("Family Manager: панель показана.")
except Exception as ex:
    logger.error("Failed to toggle pane: {0}".format(ex))
    forms.alert("Не удалось переключить панель:\n{0}".format(ex),
                title="Family Manager", warn_icon=True)
