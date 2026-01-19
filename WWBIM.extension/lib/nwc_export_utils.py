# -*- coding: utf-8 -*-
"""
nwc_export_utils.py — общий модуль для экспорта RVT в NWC.
Извлекает общие функции из navis_export_script.py для переиспользования.
"""

import os
import datetime
from pyrevit import coreutils

# Revit API
from Autodesk.Revit.DB import (
    ModelPathUtils,
    View3D,
    ViewFamilyType,
    ViewFamily,
    FilteredElementCollector,
    CategoryType,
    BuiltInParameter,
    Transaction,
    NavisworksExportOptions,
    NavisworksExportScope,
    Category,
    ElementId,
    ImportInstance,
)
from System import Enum
from System.Collections.Generic import List

# ваши либы
import openbg
import closebg


# ---------- helpers ----------


def to_model_path(user_visible_path):
    """Преобразование пути пользователя в ModelPath для Revit API."""
    if not user_visible_path:
        return None
    try:
        return ModelPathUtils.ConvertUserVisiblePathToModelPath(user_visible_path)
    except Exception:
        return None


def default_export_root():
    """Возвращает папку по умолчанию для экспорта NWC."""
    docs = os.path.join(os.path.expanduser("~"), "Documents")
    root = os.path.join(docs, "NWC_Export")
    if not os.path.exists(root):
        try:
            os.makedirs(root)
        except Exception:
            pass
    return root


# ---- SAFE BIC helpers (без getattr к BuiltInCategory) ----


def _resolve_bic(name):
    """Вернуть BuiltInCategory по строке или None, если такого имени нет в текущей версии Revit."""
    if not name:
        return None
    try:
        if Enum.IsDefined(BuiltInCategory, name):
            return Enum.Parse(BuiltInCategory, name)
    except Exception:
        pass
    return None


def _resolve_bip(name):
    """Вернуть BuiltInParameter по строке или None."""
    if not name:
        return None
    try:
        if Enum.IsDefined(BuiltInParameter, name):
            return Enum.Parse(BuiltInParameter, name)
    except Exception:
        pass
    return None


def _try_set_bip_int(element, bip_name, value):
    """Установить значение параметра по имени, если возможно."""
    bip = _resolve_bip(bip_name)
    if bip is None:
        return False
    try:
        p = element.get_Parameter(bip)
        if p and (not p.IsReadOnly):
            p.Set(int(value))
            return True
    except Exception:
        pass
    return False


def _cat_id(doc, bic):
    """Получить Id категории по BuiltInCategory."""
    if bic is None:
        return None
    try:
        cat = Category.GetCategory(doc, bic)
        if cat:
            return cat.Id
    except Exception:
        return None
    return None


def _hide_categories_by_names(doc, view, names):
    """Скрыть категории по списку имен."""
    ids = List[ElementId]()
    for nm in names or []:
        bic = _resolve_bic(nm)
        eid = _cat_id(doc, bic)
        if eid:
            ids.Add(eid)

    # скрыть все аннотации через тип категории (это стабильно во всех версиях)
    try:
        for c in doc.Settings.Categories:
            try:
                if c.CategoryType == CategoryType.Annotation:
                    ids.Add(c.Id)
            except Exception:
                pass
    except Exception:
        pass

    if ids.Count == 0:
        return 0

    hidden = 0
    try:
        view.HideCategories(ids)
        hidden = ids.Count
        return hidden
    except Exception:
        pass

    for eid in ids:
        ok = False
        try:
            view.SetCategoryHidden(eid, True)
            ok = True
        except Exception:
            try:
                view.SetCategoryHidden(eid.IntegerValue, True)
                ok = True
            except Exception:
                ok = False
        if ok:
            hidden += 1
    return hidden


def hide_annos_and_links_safe(view):
    """Безопасно скрыть аннотации, импорты и связи в виде."""
    doc = view.Document

    # View template can lock Visibility/Graphics. Detach for this export session (doc is opened detached and not saved).
    try:
        vtid = getattr(view, "ViewTemplateId", None)
        if vtid and (vtid.IntegerValue != -1):
            view.ViewTemplateId = ElementId.InvalidElementId
    except Exception:
        pass
    names = [
        "OST_RvtLinks",
        "OST_LinkInstances",
        # Все варианты импорта (DWG, DXF и др.)
        "OST_ExportLayer",
        "OST_ImportInstance",
        "OST_ImportsInFamilies",
        "OST_ImportObjectStyles",  # Импорт в семействах (стили объектов)
        "OST_Cameras",
        "OST_Views",
        "OST_Lines",
        "OST_PointClouds",
        "OST_PointCloudsHardware",
        "OST_Levels",
        "OST_Grids",
        "OST_Annotations",
        "OST_TitleBlocks",
        "OST_Viewports",
        "OST_TextNotes",
        "OST_Dimensions",
    ]
    hidden = _hide_categories_by_names(doc, view, names)
    # ВАЖНО: Отключаем чекбоксы "Показывать импортированные/аннотации на этом виде"
    try:
        # Скрыть все импортированные категории (вкладка "Импортированные категории")
        view.AreImportCategoriesHidden = True
    except Exception:
        pass
    _try_set_bip_int(view, "VIEW_SHOW_IMPORT_CATEGORIES", 0)
    _try_set_bip_int(view, "VIEW_SHOW_IMPORT_CATEGORIES_IN_VIEW", 0)

    try:
        # Скрыть все категории аннотаций (вкладка "Категории аннотаций")
        view.AreAnnotationCategoriesHidden = True
    except Exception:
        pass
    _try_set_bip_int(view, "VIEW_SHOW_ANNOTATION_CATEGORIES", 0)
    _try_set_bip_int(view, "VIEW_SHOW_ANNOTATION_CATEGORIES_IN_VIEW", 0)

    # Extra safety: explicitly hide ImportInstance elements so they won't be exported even if category flags are blocked.
    try:
        ids = List[ElementId]()
        for ii in FilteredElementCollector(doc, view.Id).OfClass(ImportInstance):
            try:
                if view.CanElementBeHidden(ii.Id):
                    ids.Add(ii.Id)
            except Exception:
                ids.Add(ii.Id)
        if ids.Count > 0:
            view.HideElements(ids)
    except Exception:
        pass

    return hidden


def find_or_create_navis_view(doc, view_name="Navisworks"):
    """Найти или создать 3D-вид для экспорта."""
    for v in FilteredElementCollector(doc).OfClass(View3D):
        try:
            if (not v.IsTemplate) and v.Name == view_name:
                # на всякий — привести вид к нужному набору скрытий
                with Transaction(doc, "Configure Navisworks view") as t:
                    t.Start()
                    hide_annos_and_links_safe(v)
                    t.Commit()
                # отключить 3D подрезку вида (Границы 3D вида)
                with Transaction(doc, "Отключить 3D подрезку") as t:
                    t.Start()
                    try:
                        v.IsSectionBoxActive = False
                    except Exception:
                        pass
                    t.Commit()
                return v, False
        except Exception:
            pass
    vft = None
    for t in FilteredElementCollector(doc).OfClass(ViewFamilyType):
        if t.ViewFamily == ViewFamily.ThreeDimensional:
            vft = t
            break
    if vft is None:
        raise Exception("Не найден тип 3D-вида для создания '{}'.".format(view_name))
    with Transaction(doc, "Создать вид Navisworks") as t:
        t.Start()
        view = View3D.CreateIsometric(doc, vft.Id)
        view.Name = view_name
        hide_annos_and_links_safe(view)
        # отключить 3D подрезку вида (Границы 3D вида)
        try:
            view.IsSectionBoxActive = False
        except Exception:
            pass
        t.Commit()
    return view, True


def count_visible_elements(doc, view):
    """Посчитать количество видимых элементов в виде."""
    return (
        FilteredElementCollector(doc, view.Id)
        .WhereElementIsNotElementType()
        .GetElementCount()
    )


def export_view_to_nwc(doc, view, target_folder, file_wo_ext):
    """Экспорт указанного вида в .nwc. Возвращает (api_ok, out_path, error_msg)."""
    if not target_folder or not file_wo_ext:
        return False, None, "Invalid parameters"

    if not os.path.exists(target_folder):
        try:
            os.makedirs(target_folder)
        except Exception as e:
            return False, None, "Failed to create folder: {}".format(str(e))

    opts = NavisworksExportOptions()
    opts.ExportScope = NavisworksExportScope.View
    opts.ViewId = view.Id
    api_ok = False
    out_path = None
    error_msg = None
    try:
        api_ok = doc.Export(target_folder, file_wo_ext, opts)
    except Exception as e:
        error_msg = str(e)
        api_ok = False
    if target_folder and file_wo_ext:
        out_path = os.path.join(target_folder, file_wo_ext + ".nwc")
    return api_ok, out_path, error_msg


def workset_filter(ws_name):
    """Предикат для фильтрации рабочих наборов при открытии.
    Возвращает True, если рабочий набор нужно открыть.
    """
    name = (ws_name or "").strip()
    # Исключаем: начинающиеся с '00_'
    if name.startswith("00_"):
        return False
    # Исключаем: содержащие 'Link' или 'Связь' (регистронезависимо)
    name_lower = name.lower()
    if "link" in name_lower or "связь" in name_lower:
        return False
    return True


def determine_nwc_filename(rvt_path, nwc_folder):
    """
    Определяет имя NWC файла для экспорта.

    Логика:
    - Если RVT имеет суффикс _RXX, проверяем наличие NWC с _NXX
    - Если NWC с _NXX существует - используем тот же суффикс
    - Иначе используем стандартное имя RVT файла

    Возвращает имя файла без расширения (например, "Файл" или "Файл_N22")
    """
    import re

    rvt_filename = os.path.splitext(os.path.basename(rvt_path))[0]

    match = re.search(r"_R(\d+)$", rvt_filename)
    if match:
        suffix = match.group(0)
        version_num = match.group(1)
        new_version_num = str(int(version_num) + 1)
        new_suffix = "_N" + new_version_num
        nwc_filename = rvt_filename.replace(suffix, new_suffix)
        nwc_path = os.path.join(nwc_folder, nwc_filename + ".nwc")

        if os.path.exists(nwc_path):
            return nwc_filename

    return rvt_filename


def export_rvt_to_nwc(rvt_path, nwc_folder, app, revit):
    """
    Экспорт RVT файла в NWC.

    Возвращает словарь с результатами:
    {
        'success': bool,
        'error': str or None,
        'warnings_count': int,
        'errors_count': int,
        'warnings': list,
        'errors': list,
        'dialogs_count': int,
        'dialogs': list,
        'exported_file': str or None,
        'file_size_mb': float or None,
        'time_open': str,
        'time_export': str,
        'vis_count': int or None,
        'import_count': int or None,
        'view_name': str or None,
        'view_created': bool
    }
    """
    result = {
        "success": False,
        "error": None,
        "warnings_count": 0,
        "errors_count": 0,
        "warnings": [],
        "errors": [],
        "dialogs_count": 0,
        "dialogs": [],
        "exported_file": None,
        "file_size_mb": None,
        "time_open": None,
        "time_export": None,
        "vis_count": None,
        "import_count": None,
        "view_name": None,
        "view_created": False,
    }

    mp = to_model_path(rvt_path)
    if mp is None:
        result["error"] = "Не удалось преобразовать путь в ModelPath"
        return result

    # Открываем через openbg с фильтрацией рабочих наборов
    t_open = coreutils.Timer()

    try:
        doc, failure_handler, dialog_suppressor = openbg.open_in_background(
            app,
            revit,
            mp,
            audit=False,
            worksets=("predicate", workset_filter),
            detach=True,
            suppress_dialogs=True,
        )
    except Exception as e:
        result["error"] = "Ошибка открытия: {}".format(e)
        result["time_open"] = str(datetime.timedelta(seconds=int(t_open.get_time())))
        return result

    open_s = str(datetime.timedelta(seconds=int(t_open.get_time())))

    # Вывод информации об обработанных предупреждениях/ошибках
    if failure_handler is not None:
        try:
            summary = failure_handler.get_summary()
            result["warnings_count"] = summary.get("total_warnings", 0)
            result["errors_count"] = summary.get("total_errors", 0)
            result["warnings"] = summary.get("warnings", [])[:5]
            result["errors"] = summary.get("errors", [])[:3]
        except Exception:
            pass

    # Вид Navisworks
    try:
        view, created = find_or_create_navis_view(doc)
        result["view_name"] = view.Name
        result["view_created"] = created
    except Exception as e:
        result["error"] = "Ошибка подготовки вида 'Navisworks': {}".format(e)
        if dialog_suppressor is not None:
            try:
                dialog_suppressor.detach()
            except Exception:
                pass
        try:
            closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
        except Exception:
            pass
        result["time_open"] = open_s
        return result

    try:
        doc.Regenerate()
    except Exception:
        pass

    # Подсчет ImportInstance в виде
    try:
        import_count = (
            FilteredElementCollector(doc, view.Id)
            .OfClass(ImportInstance)
            .GetElementCount()
        )
        result["import_count"] = import_count
    except Exception:
        pass

    vis_count = count_visible_elements(doc, view)
    result["vis_count"] = vis_count

    # Экспорт
    t_exp = coreutils.Timer()
    api_ok, out_path = False, None
    err_text = None
    try:
        if vis_count > 0:
            file_wo_ext = determine_nwc_filename(rvt_path, nwc_folder)
            api_ok, out_path, export_err = export_view_to_nwc(
                doc, view, nwc_folder, file_wo_ext
            )
            if export_err:
                err_text = export_err
        else:
            err_text = "Вид не имеет элементов."
    except Exception as e:
        err_text = str(e)

    file_ok = False
    if out_path and os.path.exists(out_path):
        file_ok = os.path.getsize(out_path) > 0

    ok = (api_ok or file_ok) and (err_text is None)
    exp_s = str(datetime.timedelta(seconds=int(t_exp.get_time())))

    if file_ok and out_path:
        try:
            result["exported_file"] = out_path
            result["file_size_mb"] = os.path.getsize(out_path) / (1024 * 1024)
        except Exception:
            pass

    # Закрытие
    try:
        closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
    except Exception:
        pass

    # Информация о подавленных диалогах
    if dialog_suppressor is not None:
        try:
            dialog_summary = dialog_suppressor.get_summary()
            result["dialogs_count"] = dialog_summary.get("total_dialogs", 0)
            result["dialogs"] = dialog_summary.get("dialogs", [])[:5]
        except Exception:
            pass
        try:
            dialog_suppressor.detach()
        except Exception:
            pass

    result["success"] = ok
    result["error"] = err_text if not ok else None
    result["time_open"] = open_s
    result["time_export"] = exp_s

    return result
