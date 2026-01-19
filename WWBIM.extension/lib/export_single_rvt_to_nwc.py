# -*- coding: utf-8 -*-
"""
export_single_rvt_to_nwc — обёртка для экспорта одного RVT файла в NWC.
Использует существующие функции из nwc_export_utils.py
"""

import os
import datetime

from pyrevit import coreutils


def export_single_rvt_to_nwc(
    rvt_path, nwc_folder, object_name, app, revit, timeout=600
):
    """
    Экспорт одного RVT файла в NWC.

    Args:
        rvt_path: путь к RVT файлу
        nwc_folder: папка для сохранения NWC
        object_name: имя объекта (для логов)
        app: Revit Application
        revit: Revit UIDocument или Application
        timeout: таймаут в секундах (по умолчанию 600)

    Returns:
        dict с результатами:
            {
                'success': bool,
                'error': str or None,
                'warnings_count': int,
                'errors_count': int,
                'warnings': list,
                'errors': list,
                'exported_file': str or None,
                'file_size_mb': float or None,
                'time_open': str,
                'time_export': str
            }
    """
    result = {
        "success": False,
        "error": None,
        "warnings_count": 0,
        "errors_count": 0,
        "warnings": [],
        "errors": [],
        "exported_file": None,
        "file_size_mb": None,
        "time_open": None,
        "time_export": None,
    }

    # Импортируем существующие функции (будут доступны если файл нахидится в том же каталоге)
    try:
        from openbg import open_in_background
        from closebg import close_with_policy
        from nwc_export_utils import export_rvt_to_nwc_full
    except ImportError as e:
        result["error"] = "Не удалось импортировать необходимые модули: {}".format(e)
        return result

    t_all = coreutils.Timer()

    try:
        # Файл и расширение
        file_wo_ext = os.path.splitext(os.path.basename(rvt_path))[0]

        # Открываем через openbg
        t_open = coreutils.Timer()
        try:
            doc, failure_handler, dialog_suppressor = open_in_background(
                app,
                revit,
                rvt_path,
                audit=False,
                worksets=("predicate", workset_filter),
                detach=True,  # Отсоединяем после открытия
                suppress_dialogs=True,  # Подавляем диалоговые окна
            )
        except Exception as e:
            result["error"] = "Ошибка открытия RVT файла: {}".format(e)
            result["time_open"] = str(
                datetime.timedelta(seconds=int(t_open.get_time()))
            )
            return result

        # Проверяем на видимые элементы
        try:
            vis_count = 0
            if doc and doc.ActiveView:
                from Autodesk.Revit.DB import FilteredElementCollector

                vis_count = (
                    FilteredElementCollector(doc, doc.ActiveView.Id)
                    .WhereElementIsNotElementType()
                    .GetElementCount()
                )
        except Exception:
            pass

        # Выполняем экспорт
        t_exp = coreutils.TIMER.Timer()
        try:
            if vis_count > 0:
                file_wo_ext = os.path.splitext(os.path.basename(rvt_path))[0]

                api_ok, out_path, export_err = export_view_to_nwc(
                    doc, doc.ActiveView, nwc_folder, file_wo_ext
                )
                if export_err:
                    result["error"] = export_err
            else:
                result["error"] = "Вид не имеет элементов."
        except Exception as e:
            result["error"] = "Ошибка экспорта: {}".format(e)

        result["time_export"] = str(datetime.timedelta(seconds=int(t_exp.get_time())))

        if api_ok or (os.path.exists(out_path) and os.path.getsize(out_path) > 0):
            result["success"] = True
            result["exported_file"] = out_path
            try:
                result["file_size_mb"] = os.path.getsize(out_path) / (1024 * 1024)
            except Exception:
                pass

        # Закрываем документ
        try:
            close_with_policy(doc, do_sync=False, save_if_not_ws=False)
        except Exception:
            pass

    except Exception as e:
        result["error"] = "Неизвестная ошибка: {}".format(e)
        result["time_open"] = str(datetime.timedelta(seconds=int(t_all.get_time())))
        return result
