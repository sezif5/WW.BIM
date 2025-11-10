# -*- coding: utf-8 -*-
__title__  = "Экспорт NWC"
__author__ = "vlad / you"
__doc__    = "Экспорт .nwc в выбранную папку (без создания подпапок). Открытие РН через openbg: все, кроме начинающихся с '00_'. Совместимость Revit 2022/2023."

import os
import datetime
from pyrevit import script, coreutils, forms

# Revit API
from Autodesk.Revit.DB import (
    ModelPathUtils, View3D, ViewFamilyType, ViewFamily,
    FilteredElementCollector, CategoryType, BuiltInCategory,
    Transaction, NavisworksExportOptions, NavisworksExportScope,
    Category, ElementId
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
        try: os.makedirs(root)
        except Exception: pass
    return root

def select_export_root():
    folder = forms.pick_folder(title=u"Выберите папку, куда складывать NWC")
    if not folder:
        folder = default_export_root()
        out.print_md(u":information_source: Папка не выбрана. Используем по умолчанию: `{}`".format(folder))
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
    for nm in (names or []):
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
    names = [
        'OST_RvtLinks', 'OST_LinkInstances',
        'OST_ExportLayer', 'OST_ImportInstance', 'OST_ImportsInFamilies',  # отсутствующие просто пропустятся
        'OST_Cameras', 'OST_Views', 'OST_Lines', 'OST_PointClouds', 'OST_PointCloudsHardware',
        'OST_Levels', 'OST_Grids',
        'OST_Annotations', 'OST_TitleBlocks', 'OST_Viewports', 'OST_TextNotes', 'OST_Dimensions'
    ]
    return _hide_categories_by_names(doc, view, names)

def find_or_create_navis_view(doc):
    for v in FilteredElementCollector(doc).OfClass(View3D):
        try:
            if (not v.IsTemplate) and v.Name == "Navisworks":
                # на всякий — привести вид к нужному набору скрытий
                hide_annos_and_links_safe(v)
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
        t.Commit()
    return view, True

def count_visible_elements(doc, view):
    return (FilteredElementCollector(doc, view.Id)
            .WhereElementIsNotElementType()
            .GetElementCount())

def export_view_to_nwc(doc, view, target_folder, file_wo_ext):
    """Экспорт указанного вида в .nwc. Возвращает (api_ok, out_path)."""
    if not os.path.exists(target_folder):
        try: os.makedirs(target_folder)
        except Exception: pass
    opts = NavisworksExportOptions()
    opts.ExportScope = NavisworksExportScope.View
    opts.ViewId      = view.Id
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
    models = forms.pick_file(files_filter="Revit files (*.rvt)|*.rvt",
                             multi_file=True,
                             title="Выберите Revit модели для экспорта")
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
        out.print_md(":open_file_folder: **{}** → {}".format(model_name, out_file_expected))

        mp = to_model_path(user_path)
        if mp is None:
            out.print_md(":x: Не удалось преобразовать путь в ModelPath. Пропуск.")
            out.update_progress(i + 1, len(sel_models))
            continue

        # Открываем через openbg: все РН, кроме начинающихся с '00_'
        t_open = coreutils.Timer()
        try:
            doc = openbg.open_in_background(
                __revit__.Application, __revit__, mp,
                audit=False,
                worksets='all_except_00'
            )
        except Exception as e:
            out.print_md(":x: Ошибка открытия: `{}`".format(e))
            out.update_progress(i + 1, len(sel_models))
            continue
        open_s = str(datetime.timedelta(seconds=int(t_open.get_time())))

        # Вид Navisworks
        try:
            view, created = find_or_create_navis_view(doc)
        except Exception as e:
            out.print_md(":x: Ошибка подготовки вида 'Navisworks': `{}`".format(e))
            try: closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
            except Exception: pass
            out.update_progress(i + 1, len(sel_models))
            continue

        vis_count  = count_visible_elements(doc, view)
        out.print_md(u"На виде **{}** видно элементов: **{}**".format(view.Name, vis_count))

        # Экспорт (в корень, без подпапки)
        t_exp = coreutils.Timer()
        api_ok, out_path = False, out_file_expected
        err_text = None
        try:
            if vis_count > 0:
                api_ok, out_path = export_view_to_nwc(doc, view, dest_folder, file_wo_ext)
            else:
                err_text = "Вид не имеет элементов."
        except Exception as e:
            err_text = str(e)

        file_ok = os.path.exists(out_path) and (os.path.getsize(out_path) > 0)
        ok = (api_ok or file_ok) and (err_text is None)
        exp_s = str(datetime.timedelta(seconds=int(t_exp.get_time())))

        if file_ok and not api_ok and err_text is None:
            out.print_md(u":warning: API вернул False, но файл существует: `{}` ({} байт)".format(
                out_path, os.path.getsize(out_path)
            ))

        # Закрытие
        try:
            closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
        except Exception:
            pass

        outcome = u":white_check_mark: OK" if ok else (u":x: Ошибка — {}".format(err_text) if err_text else u":x: Ошибка")
        out.print_md(u"- Открытие: **{}**, Экспорт: **{}** → {}".format(open_s, exp_s, outcome))
        if ok:
            out.print_md(u"Готово: `{}`".format(out_path))
        out.print_md("___")

        out.update_progress(i + 1, len(sel_models))

    all_s = str(datetime.timedelta(seconds=int(t_all.get_time())))
    out.print_md("**Готово. Время всего: {}**".format(all_s))


if __name__ == "__main__":
    main()
