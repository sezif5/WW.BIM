# -*- coding: utf-8 -*-
# startup.py — регистрация Dockable Pane для Family Manager через pyRevit
# Вариант Б: используем C# UserControl из DLL как содержимое панели.
#
# Настройте список DLL_PATHS ниже: первый существующий путь будет использован.
# Если в DLL есть единственный публичный UserControl с пустым конструктором,
# он будет найден автоматически. Иначе укажите CLASS_FULLNAME.
#
# Требования: pyRevit (IronPython), Revit 2020+ (проверено на 2023).
from __future__ import print_function

import os
from pyrevit import HOST_APP, script
from pyrevit.framework import clr
from pyrevit.coreutils import Guid
from Autodesk.Revit import UI

logger = script.get_logger()

# ------------------------- НАСТРОЙКИ -------------------------
# Укажите путь(и) к вашей сборке с UserControl.
DLL_PATHS = [
    r"Z:\02_Библиотека\03_Dynamo\Scripts\WWBIM.extension\WW.BIM.tab\bin\FamilyManager.dll",
    os.path.join(os.path.dirname(__file__), "bin", "FamilyManager.dll"),
]

# Необязательно: полное имя класса UserControl, если авто-поиск не подойдёт.
# Примеры:
#   "FamilyManager.UI.FamilyLoaderPage"
#   "FamilyManager.FamilyManagerPane"
CLASS_FULLNAME = "FamilyManager.UI.FamilyLoaderPage"  # ВАЖНО: указываем полное имя

# Постоянный GUID панели (не меняйте после первого запуска)
PANE_GUID = "A7E9F8D3-1234-4567-89AB-CDEF01234567"  # Используем тот же GUID что в App.cs
PANE_TITLE = "Family Manager"
TAB_BEHIND = UI.DockablePanes.BuiltInDockablePanes.ProjectBrowser
# TAB_BEHIND = UI.DockablePanes.BuiltInDockablePanes.PropertiesPalette
# -------------------------------------------------------------


def _load_assembly():
    """Подключить DLL (первую найденную) и вернуть объект Assembly."""
    import shutil
    
    for path in DLL_PATHS:
        if not path or not os.path.exists(path):
            continue
            
        try:
            # Сначала пробуем загрузить напрямую
            logger.info("Attempting direct load: {0}".format(path))
            asm = clr.AddReferenceToFileAndPath(path)
            logger.info("Loaded DLL directly: {0}".format(path))
            return asm
        except Exception as ex:
            logger.warning("Direct load failed: {0}".format(ex))
            
            # Если не получилось (например, из-за сетевого диска), копируем в локальный кэш
            try:
                # Создаём папку локального кэша
                cache_dir = os.path.join(
                    os.environ.get('LOCALAPPDATA', os.path.expanduser('~')),
                    'pyRevit', 'FamilyManager', 'cache'
                )
                
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                    logger.info("Created cache directory: {0}".format(cache_dir))
                
                # Определяем исходную папку с DLL
                source_dir = os.path.dirname(path)
                dll_name = os.path.basename(path)
                
                # Копируем все .dll файлы из исходной папки в кэш
                logger.info("Copying DLLs from {0} to cache...".format(source_dir))
                copied_files = []
                
                for file in os.listdir(source_dir):
                    if file.lower().endswith('.dll'):
                        src_file = os.path.join(source_dir, file)
                        dst_file = os.path.join(cache_dir, file)
                        
                        # Копируем только если файл новее или не существует
                        if not os.path.exists(dst_file) or \
                           os.path.getmtime(src_file) > os.path.getmtime(dst_file):
                            shutil.copy2(src_file, dst_file)
                            copied_files.append(file)
                            logger.info("  Copied: {0}".format(file))
                
                if copied_files:
                    logger.info("Copied {0} DLL file(s) to cache".format(len(copied_files)))
                else:
                    logger.info("All DLLs already up-to-date in cache")
                
                # Загружаем DLL из локального кэша
                cached_dll = os.path.join(cache_dir, dll_name)
                logger.info("Loading from cache: {0}".format(cached_dll))
                asm = clr.AddReferenceToFileAndPath(cached_dll)
                logger.info("Loaded DLL from cache successfully")
                return asm
                
            except Exception as cache_ex:
                logger.error("Failed to load from cache: {0}".format(cache_ex))
                import traceback
                logger.error(traceback.format_exc())
    
    raise IOError("FamilyManager.dll не найден. Проверьте DLL_PATHS в startup.py")


def _get_usercontrol_instance(asm):
    """Найти и создать экземпляр UserControl из указанной сборки."""
    import System
    from System import Array, Type
    from System.Windows.Controls import UserControl
    from System.Reflection import Assembly

    # Получаем реальный объект Assembly
    # clr.AddReferenceToFileAndPath возвращает не Assembly, а модуль Python
    # Нужно найти загруженную сборку по имени
    loaded_asm = None
    asm_name = None
    
    try:
        # Пытаемся получить имя сборки
        if hasattr(asm, 'GetName'):
            asm_name = asm.GetName().Name
        elif hasattr(asm, '__name__'):
            asm_name = asm.__name__
        else:
            # Перебираем все загруженные сборки
            for loaded in System.AppDomain.CurrentDomain.GetAssemblies():
                if 'FamilyManager' in loaded.FullName:
                    loaded_asm = loaded
                    asm_name = loaded.GetName().Name
                    logger.info("Found assembly: {0}".format(loaded.FullName))
                    break
    except Exception as ex:
        logger.warning("Failed to get assembly name: {0}".format(ex))
    
    if loaded_asm is None and asm_name:
        # Ищем сборку по имени
        for loaded in System.AppDomain.CurrentDomain.GetAssemblies():
            if loaded.GetName().Name == asm_name:
                loaded_asm = loaded
                break
    
    if loaded_asm is None:
        logger.error("Failed to find loaded assembly")
        raise TypeError("Could not find FamilyManager assembly in AppDomain")
    
    logger.info("Using assembly: {0}".format(loaded_asm.FullName))

    # Сначала выведем все типы из сборки для отладки
    logger.info("=== Available types in assembly ===")
    all_types = []
    try:
        all_types = list(loaded_asm.GetExportedTypes())
        for t in all_types:
            logger.info("  Type: {0}".format(t.FullName))
    except Exception as ex:
        logger.error("Failed to list types: {0}".format(ex))
        import traceback
        logger.error(traceback.format_exc())
    logger.info("=== End of types list ===")

    # 1) Если задан CLASS_FULLNAME — пробуем его
    if CLASS_FULLNAME:
        try:
            logger.info("Attempting to load class: {0}".format(CLASS_FULLNAME))
            # Получаем тип напрямую из сборки
            ctrl_type = loaded_asm.GetType(CLASS_FULLNAME)
            if ctrl_type is not None:
                logger.info("Type found: {0}".format(ctrl_type.FullName))
                logger.info("Type base: {0}".format(ctrl_type.BaseType))
                logger.info("Is UserControl subclass: {0}".format(ctrl_type.IsSubclassOf(UserControl)))
                
                # Пробуем создать экземпляр
                instance = ctrl_type()
                logger.info("Instance created successfully")
                return instance
            else:
                logger.warning("Type not found: {0}".format(CLASS_FULLNAME))
                logger.warning("Make sure the class name is correct and the class is public")
        except Exception as ex:
            logger.error("Failed to create {0}: {1}".format(CLASS_FULLNAME, ex))
            import traceback
            logger.error(traceback.format_exc())

    # 2) Иначе ищем любой публичный класс-наследник UserControl с пустым конструктором
    if all_types:
        try:
            logger.info("Searching for UserControl in assembly...")
            empty_sig = Array[Type]([])
            for t in all_types:
                try:
                    logger.debug("Checking type: {0}".format(t.FullName))
                    if t.IsClass and not t.IsAbstract:
                        logger.debug("  - Is class: True, Base: {0}".format(t.BaseType))
                        if t.IsSubclassOf(UserControl):
                            logger.info("  - Found UserControl subclass!")
                            if t.GetConstructor(empty_sig) is not None:
                                logger.info("Auto-detected control type: {0}".format(t.FullName))
                                return t()
                except Exception as ex:
                    logger.debug("Skipped type {0}: {1}".format(t.FullName if hasattr(t, 'FullName') else t, ex))
                    continue
        except Exception as ex:
            logger.error("Error searching for UserControl in DLL: {0}".format(ex))
            import traceback
            logger.error(traceback.format_exc())

    raise TypeError("No suitable UserControl found in DLL. "
                    "Set CLASS_FULLNAME in startup.py or ensure your class inherits from UserControl")


# Модульные синглтоны, чтобы экземпляры не сборосились GC
_CTRL_INSTANCE = None
_PANE_ID = UI.DockablePaneId(Guid(PANE_GUID))


class _Provider(UI.IDockablePaneProvider):
    """Провайдер для Dockable Pane."""
    def SetupDockablePane(self, data):
        global _CTRL_INSTANCE
        # Устанавливаем контрол как содержимое панели
        data.FrameworkElement = _CTRL_INSTANCE
        
        # Настройка начального положения
        state = UI.DockablePaneState()
        state.DockPosition = UI.DockPosition.Tabbed
        state.TabBehind = TAB_BEHIND
        data.InitialState = state
        data.VisibleByDefault = False


def _register_pane():
    """Регистрация панели в Revit через pyRevit HOST_APP.uiapp."""
    uiapp = HOST_APP.uiapp
    try:
        uiapp.RegisterDockablePane(_PANE_ID, PANE_TITLE, _Provider())
        logger.info("Dockable Pane registered: {0}".format(PANE_TITLE))
        
        # Инициализируем контрол с UIApplication сразу после регистрации
        global _CTRL_INSTANCE
        if _CTRL_INSTANCE and hasattr(_CTRL_INSTANCE, 'Initialize'):
            try:
                _CTRL_INSTANCE.Initialize(uiapp)
                logger.info("Control initialized with UIApplication")
            except Exception as ex:
                logger.warning("Failed to initialize control: {0}".format(ex))
                
    except Exception as ex:
        # Если уже зарегистрирована — просто сообщим в лог и продолжим
        msg = str(ex)
        if "already registered" in msg.lower():
            logger.debug("Pane already registered: {0}".format(PANE_TITLE))
        else:
            logger.warning("RegisterDockablePane note: {0}".format(ex))


def _ensure_loaded():
    """Главный вход: загрузить DLL, создать контрол и зарегистрировать панель."""
    global _CTRL_INSTANCE
    try:
        logger.info("=" * 60)
        logger.info("FamilyManager Dockable Pane initialization starting...")
        logger.info("=" * 60)
        
        asm = _load_assembly()
        logger.info("Assembly loaded successfully")
        
        _CTRL_INSTANCE = _get_usercontrol_instance(asm)
        logger.info("UserControl instance created")
        
        _register_pane()
        logger.info("Pane registration completed")
        
        logger.info("=" * 60)
        logger.info("FamilyManager Dockable Pane initialization SUCCESSFUL")
        logger.info("=" * 60)
    except Exception as ex:
        logger.error("=" * 60)
        logger.error("FamilyManager Dockable Pane init FAILED: {0}".format(ex))
        logger.error("=" * 60)
        import traceback
        logger.error(traceback.format_exc())


# Выполняем при старте pyRevit
logger.info("Starting FamilyManager startup.py...")
_ensure_loaded()
