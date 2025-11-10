# -*- coding: utf-8 -*-
__title__  = "Экспорт RVT"
__author__ = "vlad / you"
__doc__    = ("Открывает модели с Revit Server в фоне и сохраняет в локальную папку.\n"
              "Режимы: preserve (Detach&Preserve + SaveAsCentral), "
              "discard (Detach&Discard), none (без Detach).")

import os, datetime
from pyrevit import script, coreutils, forms

# Revit API
from Autodesk.Revit.DB import (
    ModelPathUtils, ModelPath, OpenOptions, DetachFromCentralOption,
    SaveAsOptions, WorksharingSaveAsOptions
)

# ваши модули
import openbg   # политика РН и фоновое открытие (использую _build_ws_config/open_in_background)  # noqa
import closebg  # корректное закрытие/синхронизация                                                    # noqa

# ---------------- настройки ----------------
DETACH_MODE        = "preserve"       # "preserve" | "discard" | "none"
OPEN_WORKSETS_RULE = "all_except_00"  # как в твоём NWC-скрипте
COMPACT_ON_SAVE    = True
OVERWRITE_SAME     = True

# ---------------- helpers ----------------
out = script.get_output()
out.close_others(all_open_outputs=True)

def to_model_path(user_visible_path):
    if not user_visible_path:
        return None
    try:
        return ModelPathUtils.ConvertUserVisiblePathToModelPath(user_visible_path)
    except Exception:
        return None

def default_export_root():
    root = os.path.join(os.path.expanduser("~"), "Documents", "RVT_Detached")
    if not os.path.exists(root):
        try: os.makedirs(root)
        except Exception: pass
    return root

def select_export_root():
    folder = forms.pick_folder(title=u"Куда сохранить RVT")
    if not folder:
        folder = default_export_root()
        out.print_md(u":information_source: Папка не выбрана. Используем: `{}`".format(folder))
    return os.path.normpath(folder)

def pick_models():
    # сперва пробуем твой кастомный селектор (как в экспортёре NWC)
    try:
        from sup import select_file as pick_models_custom
        models = pick_models_custom() or []
        return list(models)
    except Exception:
        pass
    models = forms.pick_file(files_filter="Revit files (*.rvt)|*.rvt",
                             multi_file=True,
                             title="Выберите Revit-модели (можно RSN://)")
    return list(models) if models else []

def model_name_from_path(p):
    try: return os.path.basename(p)
    except Exception: return "Model.rvt"

# ---------------- открытие ----------------
def open_document(mp, worksets_rule):
    app = __revit__.Application
    ui  = __revit__
    opts = OpenOptions()
    opts.Audit = False

    if DETACH_MODE == "preserve":
        opts.DetachFromCentralOption = DetachFromCentralOption.DetachAndPreserveWorksets
        try:
            cfg = openbg._build_ws_config(ui, mp, worksets_rule)  # :contentReference[oaicite:1]{index=1}
            opts.SetOpenWorksetsConfiguration(cfg)
        except Exception:
            pass
        return app.OpenDocumentFile(mp, opts)

    if DETACH_MODE == "discard":
        opts.DetachFromCentralOption = DetachFromCentralOption.DetachAndDiscardWorksets
        return app.OpenDocumentFile(mp, opts)

    # DETACH_MODE == "none"
    return openbg.open_in_background(app, ui, mp, audit=False, worksets=worksets_rule)  # :contentReference[oaicite:2]{index=2}

# ---------------- сохранение ----------------
def save_document(doc, full_path):
    # гарантируем наличие директории
    dst_dir = os.path.dirname(full_path)
    if not os.path.exists(dst_dir):
        try: os.makedirs(dst_dir)
        except Exception: pass

    # Лучше использовать ModelPath (работает и с локальными путями)
    mp_out = ModelPathUtils.ConvertUserVisiblePathToModelPath(full_path)

    sao = SaveAsOptions()
    sao.Compact = bool(COMPACT_ON_SAVE)
    sao.OverwriteExistingFile = bool(OVERWRITE_SAME)

    if DETACH_MODE == "preserve":
        # ВКЛЮЧИТЬ worksharing-опции ВНУТРЬ SaveAsOptions
        wsa = WorksharingSaveAsOptions()
        wsa.SaveAsCentral = True  # <-- ключ к сохранению «с сохранением РН»
        sao.SetWorksharingOptions(wsa)  # <-- правильный способ передать WSA
        doc.SaveAs(mp_out, sao)         # <-- только 2 аргумента

    else:
        # discard/none -> обычный не-центральный файл
        doc.SaveAs(mp_out, sao)

# ---------------- main ----------------
def main():
    sel_models = pick_models()
    if not sel_models:
        script.exit()

    export_root = select_export_root()

    out.print_md("## Сохранение RVT ({} шт.)".format(len(sel_models)))
    out.print_md("Папка: **{}**".format(export_root))
    out.print_md("Режим Detach: `{}`; РН: `{}`; Compact: {}; Overwrite: {}"
                 .format(DETACH_MODE, OPEN_WORKSETS_RULE, COMPACT_ON_SAVE, OVERWRITE_SAME))
    out.print_md("___")

    total_timer = coreutils.Timer()
    out.update_progress(0, len(sel_models))

    for i, user_path in enumerate(sel_models):
        model_file  = model_name_from_path(user_path)
        name_wo_ext = os.path.splitext(model_file)[0]
        dst_file    = os.path.join(export_root, name_wo_ext + ".rvt")

        out.print_md(u":open_file_folder: **{}** → {}".format(model_file, dst_file))

        mp = to_model_path(user_path)
        if mp is None:
            out.print_md(":x: Не удалось преобразовать путь в ModelPath. Пропуск.")
            out.update_progress(i + 1, len(sel_models)); continue

        # Открытие
        t_open = coreutils.Timer()
        try:
            doc = open_document(mp, OPEN_WORKSETS_RULE)
        except Exception as e:
            out.print_md(":x: Ошибка открытия: `{}`".format(e))
            out.update_progress(i + 1, len(sel_models)); continue
        open_s = str(datetime.timedelta(seconds=int(t_open.get_time())))

        # Сохранение
        t_save = coreutils.Timer()
        ok, err = True, None
        try:
            save_document(doc, dst_file)
        except Exception as e:
            ok, err = False, str(e)
        save_s = str(datetime.timedelta(seconds=int(t_save.get_time())))

        # Закрытие (без sync на сервер)
        try:
            closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)  # :contentReference[oaicite:3]{index=3}
        except Exception:
            pass

        outcome = u":white_check_mark: OK" if ok else u":x: Ошибка — {}".format(err)
        out.print_md(u"- Открытие: **{}**, Сохранение: **{}** → {}".format(open_s, save_s, outcome))
        if ok and os.path.exists(dst_file):
            out.print_md(u"Готово: `{}`".format(dst_file))
        out.print_md("___")
        out.update_progress(i + 1, len(sel_models))

    all_s = str(datetime.timedelta(seconds=int(total_timer.get_time())))
    out.print_md("**Готово. Время всего: {}**".format(all_s))

if __name__ == "__main__":
    main()
