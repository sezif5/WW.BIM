# -*- coding: utf-8 -*-
__title__ = "Экспорт NWC"
__author__ = "vlad / you"
__doc__ = "Экспорт .nwc в выбранную папку (без создания подпапок). Открытие РН через openbg: все, кроме начинающихся с '00_' и содержащих 'Link'/'Связь'. Совместимость Revit 2022/2023."

import os
import datetime
from pyrevit import script, coreutils, forms

# Revit API
from Autodesk.Revit.DB import (
    ModelPathUtils,
    View3D,
    ViewFamilyType,
    ViewFamily,
    FilteredElementCollector,
    CategoryType,
    BuiltInCategory,
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

SAVE_CREATED_VIEW = False

out = script.get_output()
out.close_others(all_open_outputs=True)

# ---------- helpers ----------


def to_model_path(user_visible_path):
    if not user_visible_path:
        return None
    try:
        return ModelPathUtils.ConvertUserVisiblePathToModelPath(user_visible_path)
    except Exception:
        return None


def default_export_root():
    docs = os.path.join(os.path.expanduser("~"), "Documents")
    root = os.path.join(docs, "NWC_Export")
    if not os.path.exists(root):
        try:
            os.makedirs(root)
        except Exception:
            pass
    return root


def select_export_root():
    folder = forms.pick_folder(title="Выберите папку, куда складывать NWC")
    if not folder:
        folder = default_export_root()
        out.print_md(
            ":information_source: Папка не выбрана. Используем по умолчанию: `{}`".format(
                folder
            )
        )
    return os.path.normpath(folder)


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
    if not name:
        return None
    try:
        if Enum.IsDefined(BuiltInParameter, name):
            return Enum.Parse(BuiltInParameter, name)
    except Exception:
        pass
    return None


def _try_set_bip_int(element, bip_name, value):
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


def find_or_create_navis_view(doc):
    for v in FilteredElementCollector(doc).OfClass(View3D):
        try:
            if (not v.IsTemplate) and v.Name == "Navisworks":
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
        raise Exception("Не найден тип 3D-вида для создания 'Navisworks'.")
    with Transaction(doc, "Создать вид Navisworks") as t:
        t.Start()
        view = View3D.CreateIsometric(doc, vft.Id)
        view.Name = "Navisworks"
        hide_annos_and_links_safe(view)
        # отключить 3D подрезку вида (Границы 3D вида)
        try:
            view.IsSectionBoxActive = False
        except Exception:
            pass
        t.Commit()
    return view, True


def count_visible_elements(doc, view):
    return (
        FilteredElementCollector(doc, view.Id)
        .WhereElementIsNotElementType()
        .GetElementCount()
    )


def export_view_to_nwc(doc, view, target_folder, file_wo_ext):
    """Экспорт указанного вида в .nwc. Возвращает (api_ok, out_path)."""
    if not target_folder or not file_wo_ext:
        return False, None

    if not os.path.exists(target_folder):
        try:
            os.makedirs(target_folder)
        except Exception:
            return False, None

    opts = NavisworksExportOptions()
    opts.ExportScope = NavisworksExportScope.View
    opts.ViewId = view.Id
    api_ok = False
    try:
        api_ok = doc.Export(target_folder, file_wo_ext, opts)
    except Exception:
        api_ok = False
    out_path = os.path.join(target_folder, file_wo_ext + ".nwc")
        return api_ok, out_path


def pick_models():
    try:
        from sup import select_file as pick_models_custom
    except Exception:
        pick_models_custom = None
    if callable(pick_models_custom):
        try:
            models = pick_models_custom() or []
            return list(models)
        except Exception:
            pass
    models = forms.pick_file(
        files_filter="Revit files (*.rvt)|*.rvt",
        multi_file=True,
        title="Выберите Revit модели для экспорта",
    )
    return list(models) if models else []


# ---------- main ----------


def main():
    sel_models = pick_models()
    if not sel_models:
        script.exit()

    export_root = select_export_root()
    out.print_md("## ЭКСПОРТ NWC ({})".format(len(sel_models)))
    out.print_md("Папка экспорта: **{}**".format(export_root))
    out.print_md("___")

    t_all = coreutils.Timer()
    out.update_progress(0, len(sel_models))

    for i, user_path in enumerate(sel_models):
        model_name = os.path.basename(user_path)
        file_wo_ext = os.path.splitext(model_name)[0]
        dest_folder = export_root  # <-- БЕЗ подпапки под модель

        out_file_expected = os.path.join(dest_folder, file_wo_ext + ".nwc")
        out.print_md(
            ":open_file_folder: **{}** → {}".format(model_name, out_file_expected)
        )

        mp = to_model_path(user_path)
        if mp is None:
            out.print_md(":x: Не удалось преобразовать путь в ModelPath. Пропуск.")
            out.update_progress(i + 1, len(sel_models))
            continue

        # Открываем через openbg: все РН, кроме начинающихся с '00_' и содержащих 'Link'/'Связь'
        t_open = coreutils.Timer()

        # Функция-предикат для фильтрации рабочих наборов
        def workset_filter(ws_name):
            """Возвращает True, если рабочий набор нужно открыть"""
            name = (ws_name or "").strip()
            # Исключаем: начинающиеся с '00_'
            if name.startswith("00_"):
                return False
            # Исключаем: содержащие 'Link' или 'Связь' (регистронезависимо)
            name_lower = name.lower()
            if "link" in name_lower or "связь" in name_lower:
                return False
            return True

        try:
            doc, failure_handler, dialog_suppressor = openbg.open_in_background(
                __revit__.Application,
                __revit__,
                mp,
                audit=False,
                worksets=("predicate", workset_filter),
                detach=True,  # Отсоединить с сохранением рабочих наборов
                suppress_dialogs=True,  # Подавлять диалоговые окна (TaskDialog)
            )
        except Exception as e:
            out.print_md(":x: Ошибка открытия: `{}`".format(e))
            out.update_progress(i + 1, len(sel_models))
            continue
        open_s = str(datetime.timedelta(seconds=int(t_open.get_time())))

        # Вывод информации об обработанных предупреждениях/ошибках
        if failure_handler is not None:
            try:
                summary = failure_handler.get_summary()
                total_w = summary.get("total_warnings", 0)
                total_e = summary.get("total_errors", 0)
                if total_w > 0 or total_e > 0:
                    out.print_md(
                        ":warning: При открытии обработано автоматически: **{} предупреждений, {} ошибок**".format(
                            total_w, total_e
                        )
                    )
                    # Вывод первых 5 предупреждений
                    if total_w > 0:
                        warnings = summary.get("warnings", [])
                        for idx, w in enumerate(warnings[:5], 1):
                            out.print_md("  {}. {}".format(idx, w))
                        if total_w > 5:
                            out.print_md(
                                "  ... и ещё {} предупреждений".format(total_w - 5)
                            )
                    # Вывод первых 3 ошибок
                    if total_e > 0:
                        errors = summary.get("errors", [])
                        for idx, err in enumerate(errors[:3], 1):
                            out.print_md("  Ошибка {}: {}".format(idx, err))
                        if total_e > 3:
                            out.print_md("  ... и ещё {} ошибок".format(total_e - 3))
            except Exception:
                pass

        # Вид Navisworks
        try:
            view, created = find_or_create_navis_view(doc)
        except Exception as e:
            out.print_md(":x: Ошибка подготовки вида 'Navisworks': `{}`".format(e))
            # Отключаем подавитель диалогов
            if dialog_suppressor is not None:
                try:
                    dialog_suppressor.detach()
                except Exception:
                    pass
            try:
                closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
            except Exception:
                pass
            out.update_progress(i + 1, len(sel_models))
            continue

        try:
            doc.Regenerate()
        except Exception:
            pass

        try:
            out.print_md(
                "- Импортированные категории скрыты: **{}**".format(
                    view.AreImportCategoriesHidden
                )
            )
        except Exception:
            pass
        try:
            imp_count = (
                FilteredElementCollector(doc, view.Id)
                .OfClass(ImportInstance)
                .GetElementCount()
            )
            out.print_md("- ImportInstance в виде: **{}**".format(imp_count))
        except Exception:
            pass

        vis_count = count_visible_elements(doc, view)
        out.print_md(
            "На виде **{}** видно элементов: **{}**".format(view.Name, vis_count)
        )

        # Экспорт (в корень, без подпапки)
        t_exp = coreutils.Timer()
        api_ok, out_path = False, out_file_expected
        err_text = None
        try:
            if vis_count > 0:
                api_ok, out_path = export_view_to_nwc(
                    doc, view, dest_folder, file_wo_ext
                )
            else:
                err_text = "Вид не имеет элементов."
        except Exception as e:
            err_text = str(e)

        file_ok = os.path.exists(out_path) and (os.path.getsize(out_path) > 0)
        ok = (api_ok or file_ok) and (err_text is None)
        exp_s = str(datetime.timedelta(seconds=int(t_exp.get_time())))

        if file_ok and not api_ok and err_text is None:
            out.print_md(
                ":warning: API вернул False, но файл существует: `{}` ({} байт)".format(
                    out_path, os.path.getsize(out_path)
                )
            )

        # Закрытие
        try:
            closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
        except Exception:
            pass

        # Отключаем подавитель диалогов и выводим информацию о подавленных диалогах
        if dialog_suppressor is not None:
            try:
                dialog_summary = dialog_suppressor.get_summary()
                total_dialogs = dialog_summary.get("total_dialogs", 0)
                if total_dialogs > 0:
                    out.print_md(
                        ":speech_balloon: Автоматически закрыто диалогов: **{}**".format(
                            total_dialogs
                        )
                    )
                    dialogs = dialog_summary.get("dialogs", [])
                    for idx, d in enumerate(dialogs[:5], 1):
                        dialog_id = d.get("dialog_id", "Unknown")
                        out.print_md("  {}. {}".format(idx, dialog_id))
                    if total_dialogs > 5:
                        out.print_md(
                            "  ... и ещё {} диалогов".format(total_dialogs - 5)
                        )
            except Exception:
                pass
            try:
                dialog_suppressor.detach()
            except Exception:
                pass

        outcome = (
            ":white_check_mark: OK"
            if ok
            else (":x: Ошибка — {}".format(err_text) if err_text else ":x: Ошибка")
        )
        out.print_md(
            "- Открытие: **{}**, Экспорт: **{}** → {}".format(open_s, exp_s, outcome)
        )
        if ok:
            out.print_md("Готово: `{}`".format(out_path))
        out.print_md("___")

        out.update_progress(i + 1, len(sel_models))

    all_s = str(datetime.timedelta(seconds=int(t_all.get_time())))
    out.print_md("**Готово. Время всего: {}**".format(all_s))


if __name__ == "__main__":
    main()
