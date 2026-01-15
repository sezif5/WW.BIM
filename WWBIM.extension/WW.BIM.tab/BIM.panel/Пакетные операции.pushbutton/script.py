# -*- coding: utf-8 -*-
"""
Пакетные операции с моделями: Открыть, Загрузить семейство, Добавить связь.
Использует openbg/closebg из библиотеки.
"""
from __future__ import print_function, division

import os
import datetime
from pyrevit import script, forms

from Autodesk.Revit.DB import (
    ModelPathUtils, Transaction, FilteredElementCollector,
    Family, RevitLinkOptions, RevitLinkType, RevitLinkInstance,
    ImportPlacement,
    OpenOptions, WorksetConfiguration, WorksetConfigurationOption,
    WorksharingUtils, WorksetId
)
from System.Collections.Generic import List

import openbg
import closebg

out = script.get_output()
out.close_others(all_open_outputs=True)

# ---------- Пути ----------

def _norm(p):
    return os.path.normpath(os.path.abspath(p)) if p else p


def _module_dir():
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.getcwd()


def _find_scripts_root(start_dir):
    cur = _norm(start_dir)
    for _ in range(0, 8):
        if os.path.basename(cur).lower() == 'scripts':
            return cur
        parent = os.path.dirname(cur)
        if not parent or parent == cur:
            break
        cur = parent
    return _norm(os.path.join(start_dir, os.pardir, os.pardir))


_env_root = os.environ.get('WW_SCRIPTS_ROOT')
if _env_root and os.path.isdir(_env_root):
    SCRIPTS_ROOT = _norm(_env_root)
else:
    SCRIPTS_ROOT = _find_scripts_root(_module_dir())

OBJECTS_DIR = _norm(os.path.join(SCRIPTS_ROOT, 'Objects'))

# ---------- Helpers ----------

def to_model_path(user_visible_path):
    if not user_visible_path:
        return None
    try:
        return ModelPathUtils.ConvertUserVisiblePathToModelPath(user_visible_path)
    except Exception:
        return None


def list_txt_files(folder_path):
    """Получить список txt файлов без расширения."""
    lst = []
    try:
        for f in os.listdir(folder_path):
            if f.lower().endswith('.txt'):
                lst.append(f[:-4])
    except OSError:
        pass
    return lst


def read_models_from_txt(file_path):
    """Прочитать список моделей из txt файла."""
    models = []
    try:
        with open(file_path, 'rb') as f:
            for raw in f:
                try:
                    line = raw.decode('utf-8').strip()
                except Exception:
                    try:
                        line = raw.decode('cp1251').strip()
                    except Exception:
                        line = raw.strip()
                if line:
                    models.append(line)
    except Exception:
        pass
    return models


def get_open_families():
    """Получить список открытых семейств (документов-семейств)."""
    app = __revit__.Application
    families = []
    for doc in app.Documents:
        try:
            if doc.IsFamilyDocument:
                families.append(doc)
        except Exception:
            pass
    return families


# ---------- Действия ----------

def should_close_workset(ws_name):
    """Проверить, нужно ли закрыть рабочий набор."""
    name = (ws_name or u"").strip()
    name_lower = name.lower()
    # Закрываем если начинается с 00_ или содержит Связь/Links
    if name.startswith(u"00_"):
        return True
    if u"связь" in name_lower:
        return True
    if u"link" in name_lower:
        return True
    return False


def get_worksets_to_open(uiapp, mp):
    """Получить список ID рабочих наборов, которые нужно открыть."""
    try:
        previews = WorksharingUtils.GetUserWorksetInfo(mp)
    except Exception:
        try:
            previews = WorksharingUtils.GetUserWorksetInfoForOpen(uiapp, mp)
        except Exception:
            return None

    if not previews:
        return None

    ids_to_open = List[WorksetId]()
    for p in previews:
        try:
            if not should_close_workset(p.Name):
                ids_to_open.Add(p.Id)
        except Exception:
            pass

    return ids_to_open


def action_open_models(models):
    """Открыть выбранные модели с созданием локальной копии."""
    out.print_md(u"## ОТКРЫТИЕ МОДЕЛЕЙ ({})".format(len(models)))
    out.print_md(u"Создание локальных копий, закрытие РН: 00_*, Связь, Links")
    out.print_md(u"---")

    uiapp = __revit__
    app = uiapp.Application

    for i, model_path in enumerate(models):
        model_name = os.path.basename(model_path)
        out.print_md(u":open_file_folder: **{}**".format(model_name))

        mp = to_model_path(model_path)
        if mp is None:
            out.print_md(u":x: Не удалось преобразовать путь.")
            out.update_progress(i + 1, len(models))
            continue

        try:
            # Настройки открытия
            opts = OpenOptions()

            # Получить рабочие наборы для открытия
            ws_ids = get_worksets_to_open(uiapp, mp)
            if ws_ids is not None and ws_ids.Count > 0:
                # Закрыть все, потом открыть нужные
                ws_config = WorksetConfiguration(WorksetConfigurationOption.CloseAllWorksets)
                ws_config.Open(ws_ids)
                opts.SetOpenWorksetsConfiguration(ws_config)
            else:
                # Если не удалось получить РН - открыть все
                ws_config = WorksetConfiguration(WorksetConfigurationOption.OpenAllWorksets)
                opts.SetOpenWorksetsConfiguration(ws_config)

            # Открыть с созданием локальной копии (OpenAndActivateDocument создаёт локальную)
            uidoc = uiapp.OpenAndActivateDocument(mp, opts, False)

            if uidoc and uidoc.Document:
                out.print_md(u":white_check_mark: Открыто: **{}**".format(uidoc.Document.Title))
            else:
                out.print_md(u":warning: Документ открыт, но не активирован")

        except Exception as e:
            out.print_md(u":x: Ошибка: `{}`".format(e))

        out.update_progress(i + 1, len(models))

    out.print_md(u"---")
    out.print_md(u"**Готово.**")


def action_load_family(models):
    """Загрузить семейства из открытых документов в выбранные модели."""
    # Получить открытые семейства
    open_families = get_open_families()

    if not open_families:
        forms.alert(u"Нет открытых семейств для загрузки.", warn_icon=True)
        return

    # Выбрать семейства (multiselect)
    class FamilyItem:
        def __init__(self, doc):
            self.doc = doc
            self.name = doc.Title
        def __str__(self):
            return self.name

    family_items = [FamilyItem(d) for d in open_families]
    selected = forms.SelectFromList.show(
        family_items,
        title=u"Выберите семейства для загрузки",
        multiselect=True,
        button_name=u"Выбрать"
    )

    if not selected:
        return

    # Преобразовать в список если выбрано одно
    if not isinstance(selected, list):
        selected = [selected]

    family_docs = [item.doc for item in selected]
    family_names = [item.name for item in selected]

    out.print_md(u"## ЗАГРУЗКА СЕМЕЙСТВ")
    out.print_md(u"**Семейства ({}):** {}".format(len(family_docs), u", ".join(family_names)))
    out.print_md(u"**Моделей:** {}".format(len(models)))
    out.print_md(u"---")

    success_models = 0
    for i, model_path in enumerate(models):
        model_name = os.path.basename(model_path)
        out.print_md(u":open_file_folder: **{}**".format(model_name))

        mp = to_model_path(model_path)
        if mp is None:
            out.print_md(u":x: Не удалось преобразовать путь.")
            out.update_progress(i + 1, len(models))
            continue

        try:
            doc, _ = openbg.open_in_background(
                __revit__.Application, __revit__, mp,
                audit=False,
                worksets='all',
                detach=False,
                suppress_warnings=True
            )

            # Загрузить все выбранные семейства
            loaded_count = 0
            t = Transaction(doc, u"Загрузка семейств")
            t.Start()
            try:
                for family_doc in family_docs:
                    try:
                        loaded_family = doc.LoadFamily(family_doc)
                        if loaded_family:
                            loaded_count += 1
                    except Exception:
                        pass
                t.Commit()

                if loaded_count == len(family_docs):
                    out.print_md(u":white_check_mark: Загружено семейств: {}/{}".format(loaded_count, len(family_docs)))
                    success_models += 1
                elif loaded_count > 0:
                    out.print_md(u":warning: Загружено семейств: {}/{} (остальные уже существуют)".format(loaded_count, len(family_docs)))
                    success_models += 1
                else:
                    out.print_md(u":warning: Семейства уже существуют или не загружены")

            except Exception as e:
                t.RollBack()
                out.print_md(u":x: Ошибка загрузки: `{}`".format(e))

            # Синхронизация и закрытие
            closebg.close_with_policy(doc, do_sync=True, comment=u"Загрузка семейств")

        except Exception as e:
            out.print_md(u":x: Ошибка открытия: `{}`".format(e))

        out.update_progress(i + 1, len(models))

    out.print_md(u"---")
    out.print_md(u"**Готово. Моделей обработано: {}/{}**".format(success_models, len(models)))


def select_links_from_txt():
    """Выбрать связи через txt файлы из папки Objects."""
    # 1. Выбор объекта (txt файла)
    txt_files = list_txt_files(OBJECTS_DIR)

    if not txt_files:
        forms.alert(
            u"Папка с объектами пуста или недоступна:\n{}".format(OBJECTS_DIR),
            warn_icon=True
        )
        return None

    selected_object = forms.SelectFromList.show(
        txt_files,
        title=u"Выберите объект (для связей)",
        width=400,
        button_name=u"Далее"
    )

    if not selected_object:
        return None

    # 2. Выбор связей из списка
    txt_path = os.path.join(OBJECTS_DIR, selected_object + ".txt")
    links_list = read_models_from_txt(txt_path)

    if not links_list:
        forms.alert(u"Файл пуст или не найден: {}".format(txt_path), warn_icon=True)
        return None

    selected_links = forms.SelectFromList.show(
        links_list,
        title=u"Выберите связи для добавления",
        multiselect=True,
        width=800,
        height=600,
        button_name=u"Выбрать"
    )

    return selected_links


def get_existing_links(doc):
    """Получить список имён существующих связей в документе."""
    links = FilteredElementCollector(doc).OfClass(RevitLinkInstance).ToElements()
    names = set()
    for link in links:
        try:
            # Имя инстанса часто "file.rvt : location"
            name = link.Name.split(" ")[0]
            names.add(name)
        except Exception:
            pass
    return names


def add_single_link(doc, path_str, existing_links):
    """Добавить одну связь. Сначала Shared, при ошибке — Origin.
    Возвращает (success, message).
    """
    name_model = os.path.basename(path_str)

    # Проверка на дубликат
    if name_model in existing_links:
        return False, u"уже существует"

    mp = ModelPathUtils.ConvertUserVisiblePathToModelPath(path_str)
    rlo = RevitLinkOptions(False)

    try:
        # Создаём тип связи
        rl_type = RevitLinkType.Create(doc, mp, rlo)

        try:
            # Пробуем по общим координатам
            rl_inst = RevitLinkInstance.Create(doc, rl_type.ElementId, ImportPlacement.Shared)
            return True, u"по общим координатам"
        except Exception as e:
            # Если не совпадают СК - по внутреннему началу
            if "coordinate system" in str(e).lower() or "координат" in str(e).lower():
                rl_inst = RevitLinkInstance.Create(doc, rl_type.ElementId, ImportPlacement.Origin)
                return True, u"в начало координат"
            else:
                raise

    except Exception as e:
        return False, unicode(e)


def action_add_link(models):
    """Добавить связи в выбранные модели."""
    # Выбрать связи через txt файлы
    selected_links = select_links_from_txt()

    if not selected_links:
        return

    link_names = [os.path.basename(l) for l in selected_links]

    out.print_md(u"## ДОБАВЛЕНИЕ СВЯЗЕЙ")
    out.print_md(u"**Связей ({}):** {}".format(len(selected_links), u", ".join(link_names[:5])))
    if len(link_names) > 5:
        out.print_md(u"*... и ещё {}*".format(len(link_names) - 5))
    out.print_md(u"**Моделей:** {}".format(len(models)))
    out.print_md(u"---")

    success_models = 0
    for i, model_path in enumerate(models):
        model_name = os.path.basename(model_path)
        out.print_md(u":open_file_folder: **{}**".format(model_name))

        mp = to_model_path(model_path)
        if mp is None:
            out.print_md(u":x: Не удалось преобразовать путь.")
            out.update_progress(i + 1, len(models))
            continue

        try:
            doc, _ = openbg.open_in_background(
                __revit__.Application, __revit__, mp,
                audit=False,
                worksets='all',
                detach=False,
                suppress_warnings=True
            )

            # Получить существующие связи
            existing_links = get_existing_links(doc)

            # Добавить связи
            added = 0
            skipped = 0
            errors = 0

            t = Transaction(doc, u"Добавление связей")
            t.Start()
            try:
                for link_path in selected_links:
                    link_name = os.path.basename(link_path)

                    # Не добавлять саму себя
                    if link_name == model_name or link_name.startswith(model_name.replace(".rvt", "")):
                        out.print_md(u"  :information_source: {} — пропуск (сама себя)".format(link_name))
                        skipped += 1
                        continue

                    success, msg = add_single_link(doc, link_path, existing_links)
                    if success:
                        out.print_md(u"  :white_check_mark: {} — {}".format(link_name, msg))
                        added += 1
                    elif msg == u"уже существует":
                        out.print_md(u"  :information_source: {} — уже существует".format(link_name))
                        skipped += 1
                    else:
                        out.print_md(u"  :x: {} — {}".format(link_name, msg))
                        errors += 1

                t.Commit()

                if added > 0:
                    success_models += 1

                out.print_md(u"  **Итого:** добавлено {}, пропущено {}, ошибок {}".format(added, skipped, errors))

            except Exception as e:
                t.RollBack()
                out.print_md(u":x: Ошибка транзакции: `{}`".format(e))

            # Синхронизация и закрытие
            closebg.close_with_policy(doc, do_sync=True, comment=u"Добавление связей")

        except Exception as e:
            out.print_md(u":x: Ошибка открытия: `{}`".format(e))

        out.update_progress(i + 1, len(models))

    out.print_md(u"---")
    out.print_md(u"**Готово. Моделей обработано: {}/{}**".format(success_models, len(models)))


# ---------- Main ----------

def main():
    # 1. Выбор действия
    actions = [
        u"Открыть модели",
        u"Загрузить семейство (из открытых)",
        u"Добавить связь"
    ]

    selected_action = forms.SelectFromList.show(
        actions,
        title=u"Выберите действие",
        width=400,
        button_name=u"Далее"
    )

    if not selected_action:
        script.exit()

    # 2. Выбор объекта (txt файла)
    txt_files = list_txt_files(OBJECTS_DIR)

    if not txt_files:
        forms.alert(
            u"Папка с объектами пуста или недоступна:\n{}\n\n"
            u"SCRIPTS_ROOT: {}".format(OBJECTS_DIR, SCRIPTS_ROOT),
            warn_icon=True
        )
        script.exit()

    selected_object = forms.SelectFromList.show(
        txt_files,
        title=u"Выберите объект",
        width=400,
        button_name=u"Далее"
    )

    if not selected_object:
        script.exit()

    # 3. Выбор моделей из списка
    txt_path = os.path.join(OBJECTS_DIR, selected_object + ".txt")
    models_list = read_models_from_txt(txt_path)

    if not models_list:
        forms.alert(u"Файл пуст или не найден: {}".format(txt_path), warn_icon=True)
        script.exit()

    selected_models = forms.SelectFromList.show(
        models_list,
        title=u"Выберите модели",
        multiselect=True,
        width=800,
        height=600,
        button_name=u"Выполнить"
    )

    if not selected_models:
        script.exit()

    # 4. Выполнение действия
    out.print_md(u"# Пакетные операции")
    out.print_md(u"**Действие:** {}".format(selected_action))
    out.print_md(u"**Объект:** {}".format(selected_object))
    out.print_md(u"")

    out.update_progress(0, len(selected_models))

    if selected_action == u"Открыть модели":
        action_open_models(selected_models)
    elif selected_action == u"Загрузить семейство (из открытых)":
        action_load_family(selected_models)
    elif selected_action == u"Добавить связь":
        action_add_link(selected_models)


if __name__ == "__main__":
    main()
