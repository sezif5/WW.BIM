# -*- coding: utf-8 -*-
__title__  = 'Добавление связей'
__author__ = 'IliaNistratov / assembled'
__doc__    = """Массово добавляет Revit-связи из выбранных файлов:
1) попытка по общим координатам,
2) если не совпадают — по внутреннему началу.
Есть проверки на "саму себя" и дубли. Лог — в pyRevit Output.
"""

from Autodesk.Revit.DB import (
    ImportPlacement,
    ModelPathUtils,
    RevitLinkInstance,
    RevitLinkOptions,
    RevitLinkType,
    FilteredElementCollector,
    Transaction,
)

from pyrevit import revit, script, coreutils
from sup import select_file
import datetime
import os

output = script.get_output ()
script.get_output().close_others(all_open_outputs=True)

user = __revit__.Application.Username
doc = __revit__.ActiveUIDocument.Document

links = FilteredElementCollector(doc).OfClass(RevitLinkInstance).ToElements()

def add_link(path_str):
    """Добавляет связь из файла path_str.
    Сначала Shared, при ошибке — Origin.
    Печатает результат в Output (с Id).
    """
    timer = coreutils.Timer()
    name_model = os.path.basename(path_str)

    mp  = ModelPathUtils.ConvertUserVisiblePathToModelPath(path_str)
    rlo = RevitLinkOptions(False)
    placement_shared = ImportPlacement.Shared
    placement_origin = ImportPlacement.Origin

    try:
        # По общим координатам
        rl_type = RevitLinkType.Create(doc, mp, rlo)
        rl_inst = RevitLinkInstance.Create(doc, rl_type.ElementId, placement_shared)
        endtime = str(datetime.timedelta(seconds=timer.get_time())).split(".")[0]
        output.print_md(
            ":white_heavy_check_mark: Связь **{}** добавлена по **общим координатам**. "
            "Id: {}. Время: **{}**".format(
                name_model, output.linkify(rl_inst.Id), endtime
            )
        )
    except Exception as e:
        # Спец-обработка несоответствия СК
        if str(e) == "The host model and the link do not share the same coordinate system.":
            rl_inst = RevitLinkInstance.Create(doc, rl_type.ElementId, placement_origin)
            endtime = str(datetime.timedelta(seconds=timer.get_time())).split(".")[0]
            output.print_md(
                ":white_heavy_check_mark: Связь **{}** добавлена в **начало координат**. "
                "Id: {}. Время: **{}**".format(
                    name_model, output.linkify(rl_inst.Id), endtime
                )
            )
        else:
            output.print_md(
                ":cross_mark: Связь **{}** проигнорирована. "
                "Причина: ({})".format(name_model, str(e))
            )


def is_there_link(name_model):
    """Есть ли уже связь с таким именем файла?"""
    for link in links:
        # Имя инстанса часто начинается с '<file>.rvt ...'
        if link.Name.split(" ")[0] == name_model:
            return True
    return False


# ---- Тело скрипта ----
sel_links = select_file()
if sel_links:
    output.print_md("##ДОБАВЛЕНИЕ СВЯЗЕЙ ({})".format(len(sel_links)))
    output.print_md("___")
    t_timer = coreutils.Timer()
    output.update_progress(0, len(sel_links))
    with Transaction(doc, 'Добавление связей') as t:
        t.Start()
        for i, l in enumerate(sel_links):
            name_model = os.path.basename(l)
            # Не даём подключить саму себя
            if name_model == doc.Title.split("_" + user)[0]:
                output.print_md(
                    ":cross_mark: Связь **{}** проигнорирована. "
                    "Причина: попытка загрузить модель в себя же!".format(name_model)
                )
                continue

            # Пропуск дубликатов
            if is_there_link(name_model):
                output.print_md("   :information_source: Связь **{}** уже существует".format(name_model))
                continue

            try:
                add_link(l)
            except Exception as e:
                output.print_md(
                    ":cross_mark: Ошибка в связи **{}**. Ошибка: {}".format(name_model, str(e))
                )
                continue

            output.update_progress(i + 1, len(sel_links))
        t.Commit()

    t_endtime = str(datetime.timedelta(seconds=t_timer.get_time())).split(".")[0]
    output.print_md("___")
    output.print_md("**Время: {}**".format(t_endtime))
else:
    output.print_md(":information_source: Файлы не выбраны.")
