# -*- coding: utf-8 -*-

import clr

from Autodesk.Revit.DB import (
    FilteredElementCollector,
    View,
    ViewSheet,
    ViewType,
    ElementId,
    ElementIdSetFilter,
    IndependentTag,
    LinkElementId,
    Dimension,
    SpotDimension,
)
from Autodesk.Revit.UI import TaskDialog
from Autodesk.Revit.UI.Selection import ObjectType

from pyrevit import script, forms

from System.Collections.Generic import List as Clist

uidoc = __revit__.ActiveUIDocument
doc = uidoc.Document
output = script.get_output()


def get_target_element():
    """Возвращает элемент: либо заранее выбранный, либо выбранный пользователем."""
    sel_ids = uidoc.Selection.GetElementIds()
    if sel_ids and sel_ids.Count == 1:
        elem_id = list(sel_ids)[0]
        elem = doc.GetElement(elem_id)
        if elem is not None:
            return elem

    try:
        ref = uidoc.Selection.PickObject(
            ObjectType.Element,
            "Выберите элемент для проверки на видах"
        )
        if ref:
            return doc.GetElement(ref.ElementId)
    except:
        return None

    return None


def build_view_sheet_map():
    """Строит словарь {ViewId.IntegerValue: [ViewSheet, ...]} для всех размещённых видов."""
    result = {}
    try:
        sheets = FilteredElementCollector(doc).OfClass(ViewSheet)
    except:
        return result

    for sheet in sheets:
        try:
            view_ids = sheet.GetAllPlacedViews()
        except:
            continue

        if not view_ids:
            continue

        for vid in view_ids:
            key = vid.IntegerValue
            lst = result.get(key)
            if lst is None:
                lst = []
                result[key] = lst
            lst.append(sheet)

    return result


def has_tag_for_element(view, element_id):
    """Проверяет, есть ли на виде хоть одна марка, ссылающаяся на элемент."""
    try:
        tag_collector = FilteredElementCollector(doc, view.Id).OfClass(IndependentTag)
    except:
        return False

    for tag in tag_collector:
        # Основной способ: GetTaggedElementIds (Revit 2020+)
        try:
            link_ids = tag.GetTaggedElementIds()
        except:
            link_ids = None

        if link_ids:
            for link_id in link_ids:
                try:
                    host_id = link_id.HostElementId
                except:
                    host_id = ElementId.InvalidElementId
                if host_id == element_id:
                    return True

        # Запасной вариант: свойство TaggedElementId (на случай старых версий)
        try:
            link_id = tag.TaggedElementId  # type: LinkElementId
            if link_id is not None:
                host_id = link_id.HostElementId
                if host_id == element_id:
                    return True
        except:
            pass

    return False


def has_dimension_for_element(view, element_id):
    """Проверяет, есть ли на виде хоть один размер, ссылающийся на элемент."""
    try:
        dim_collector = FilteredElementCollector(doc, view.Id).OfClass(Dimension)
    except:
        return False

    for dim in dim_collector:
        try:
            # Получаем References размера
            refs = dim.References
            if refs:
                for ref in refs:
                    try:
                        # Reference может ссылаться на элемент напрямую
                        ref_elem_id = ref.ElementId
                        if ref_elem_id == element_id:
                            return True
                    except:
                        pass
        except:
            pass

    return False


def has_spot_elevation_for_element(view, element_id):
    """Проверяет, есть ли на виде высотная отметка, ссылающаяся на элемент."""
    try:
        spot_collector = FilteredElementCollector(doc, view.Id).OfClass(SpotDimension)
    except:
        return False

    for spot in spot_collector:
        try:
            # SpotDimension имеет свойство References
            refs = spot.References
            if refs:
                for ref in refs:
                    try:
                        ref_elem_id = ref.ElementId
                        if ref_elem_id == element_id:
                            return True
                    except:
                        pass
        except:
            pass

    return False


def find_views_for_element(element, only_on_sheets=False, view_sheet_map=None):
    """Возвращает список кортежей (view, is_tagged, is_dimensioned, has_spot_elev), где элемент присутствует на виде."""
    if element is None:
        return []

    element_id = element.Id

    id_list = Clist[ElementId]()
    id_list.Add(element_id)
    id_filter = ElementIdSetFilter(id_list)

    all_views_list = list(FilteredElementCollector(doc).OfClass(View))
    
    # Если нужно проверять только виды на листах
    if only_on_sheets and view_sheet_map:
        views = [v for v in all_views_list if v.Id.IntegerValue in view_sheet_map]
        output.print_md(u"ℹ Применён фильтр: проверяются только виды на листах ({} из {})".format(len(views), len(all_views_list)))
        output.print_md(u"")
    else:
        views = all_views_list
        output.print_md(u"ℹ Проверяются все виды проекта: {}".format(len(views)))
        output.print_md(u"")
    
    total = len(views)
    if total == 0:
        return []

    result = []

    with forms.ProgressBar(
        title=u"Поиск видов: {value} из {max_value}",
        cancellable=True,
        step=5
    ) as pb:
        for idx, view in enumerate(views):
            # проверка на отмену пользователем (должна быть ДО обновления прогресса)
            if pb.cancelled:
                output.print_md(u"---")
                output.print_md(u"## ⚠ Проверка отменена пользователем")
                output.print_md(u"")
                return []

            # обновляем прогресс
            pb.update_progress(idx + 1, total)

            if view is None:
                continue

            # пропускаем шаблоны, листы и спецификации
            try:
                if view.IsTemplate:
                    continue
            except:
                pass

            vtype = None
            try:
                vtype = view.ViewType
            except:
                pass

            if vtype in (ViewType.Schedule, ViewType.DrawingSheet, ViewType.Report):
                continue

            # коллектор в контексте вида
            try:
                collector = FilteredElementCollector(doc, view.Id)
            except:
                continue

            try:
                collector = collector.WherePasses(id_filter)
            except:
                continue

            # есть ли элемент в этом виде вообще
            found = False
            try:
                if collector.GetElementCount() > 0:
                    found = True
            except:
                pass

            if not found:
                try:
                    it = collector.GetElementIterator()
                    it.Reset()
                    if it.MoveNext():
                        found = True
                except:
                    pass

            if not found:
                continue

            # элемент присутствует на виде — проверяем наличие марки, размеров и высотных отметок
            is_tagged = False
            is_dimensioned = False
            has_spot_elev = False
            try:
                is_tagged = has_tag_for_element(view, element_id)
            except:
                is_tagged = False
            
            try:
                is_dimensioned = has_dimension_for_element(view, element_id)
            except:
                is_dimensioned = False
            
            try:
                has_spot_elev = has_spot_elevation_for_element(view, element_id)
            except:
                has_spot_elev = False

            result.append((view, is_tagged, is_dimensioned, has_spot_elev))

    return result


def show_views_table(views_info, view_sheet_map, element):
    """Показывает список видов и листов в таблице pyRevit."""
    if not views_info:
        output.print_md(u"---")
        output.print_md(u"## ❌ Результат проверки")
        output.print_md(u"")
        output.print_md(u"<span style='color:#e74c3c; font-size:14px;'>**Элемент не найден ни на одном виде проекта.**</span>")
        output.print_md(u"")
        return

    # сортируем по имени вида
    views_info = sorted(views_info, key=lambda x: x[0].Name)

    on_sheets = []
    not_on_sheets = []

    for view, is_tagged, is_dimensioned, has_spot_elev in views_info:
        sheets = view_sheet_map.get(view.Id.IntegerValue)
        if sheets:
            on_sheets.append((view, is_tagged, is_dimensioned, has_spot_elev, sheets))
        else:
            not_on_sheets.append((view, is_tagged, is_dimensioned, has_spot_elev))

    total_views = len(views_info)
    total_tagged = sum(1 for vi in views_info if vi[1])
    total_dimensioned = sum(1 for vi in views_info if vi[2])
    total_spot_elev = sum(1 for vi in views_info if vi[3])
    total_on_sheets = len(on_sheets)

    output.print_md(u"---")
    output.print_md(u"## ✅ Результат проверки")
    output.print_md(u"")
    elem_link = output.linkify(element.Id)
    output.print_md(u"<span style='color:#27ae60; font-size:14px;'>**Элемент {} найден на {} видах:**</span>".format(elem_link, total_views))
    output.print_md(u"")

    # Виды, размещённые на листах
    if on_sheets:
        output.print_md(u"### 📄 Виды, размещённые на листах")
        output.print_md(u"")
        
        table_data = []
        for idx, (view, is_tagged, is_dimensioned, has_spot_elev, sheets) in enumerate(on_sheets, 1):
            try:
                vtitle = u"{} ({})".format(view.Name, view.ViewType)
            except:
                vtitle = view.Name

            view_link = output.linkify(view.Id, title=vtitle)

            sheet_links = []
            for sh in sheets:
                try:
                    title = u"{}  {}".format(sh.SheetNumber, sh.Name)
                except:
                    title = sh.Name
                sheet_links.append(output.linkify(sh.Id, title=title))
            sheet_cell = u"<br>".join(sheet_links) if sheet_links else u"-"

            tag_cell = u"<span style='color:#27ae60;'>✅ Да</span>" if is_tagged else u"<span style='color:#e74c3c;'>❌ Нет</span>"
            dim_cell = u"<span style='color:#27ae60;'>✅ Да</span>" if is_dimensioned else u"<span style='color:#e74c3c;'>❌ Нет</span>"
            spot_cell = u"<span style='color:#27ae60;'>✅ Да</span>" if has_spot_elev else u"<span style='color:#e74c3c;'>❌ Нет</span>"

            table_data.append([idx, view_link, sheet_cell, tag_cell, dim_cell, spot_cell])
        
        output.print_table(
            table_data,
            columns=[u"№", u"👁 Вид", u"📑 Лист(ы)", u"🏷 Марка", u"📏 Размер", u"📍 Отметка"]
        )
        output.print_md(u"")

    # Виды, не размещённые на листах
    if not_on_sheets:
        output.print_md(u"### 🧾 Виды без листов")
        output.print_md(u"")
        
        table_data = []
        for idx, (view, is_tagged, is_dimensioned, has_spot_elev) in enumerate(not_on_sheets, 1):
            try:
                vtitle = u"{} ({})".format(view.Name, view.ViewType)
            except:
                vtitle = view.Name

            view_link = output.linkify(view.Id, title=vtitle)
            tag_cell = u"<span style='color:#27ae60;'>✅ Да</span>" if is_tagged else u"<span style='color:#e74c3c;'>❌ Нет</span>"
            dim_cell = u"<span style='color:#27ae60;'>✅ Да</span>" if is_dimensioned else u"<span style='color:#e74c3c;'>❌ Нет</span>"
            spot_cell = u"<span style='color:#27ae60;'>✅ Да</span>" if has_spot_elev else u"<span style='color:#e74c3c;'>❌ Нет</span>"

            table_data.append([idx, view_link, tag_cell, dim_cell, spot_cell])
        
        output.print_table(
            table_data,
            columns=[u"№", u"👁 Вид", u"🏷 Марка", u"📏 Размер", u"📍 Отметка"]
        )
        output.print_md(u"")

    # Итог
    output.print_md(u"---")
    output.print_md(u"### 📊 Итого")
    output.print_md(u"")
    
    summary_data = [
        [u"👁 Всего видов", total_views],
        [u"🏷 С маркой", total_tagged],
        [u"📏 С размером", total_dimensioned],
        [u"📍 С отметкой", total_spot_elev],
        [u"📄 На листах", total_on_sheets],
        [u"🧾 Без листов", total_views - total_on_sheets],
    ]
    output.print_table(
        summary_data,
        columns=[u"Показатель", u"Количество"]
    )
    output.print_md(u"")


def main():
    element = get_target_element()
    if element is None:
        TaskDialog.Show(
            "Проверка видов",
            "Не удалось получить элемент. Выберите один элемент и запустите скрипт снова."
        )
        return

    view_sheet_map = build_view_sheet_map()
    
    # Проверяем количество видов
    all_views = list(FilteredElementCollector(doc).OfClass(View))
    only_on_sheets = False
    
    if len(all_views) > 200:
        result = forms.alert(
            u"Видов для проверки > 200. Проверять только виды, размещённые на листах?",
            yes=True,
            no=True,
            cancel=True
        )
        if result is None:  # Cancel
            return
        only_on_sheets = result  # True = да, False = нет
    
    views_info = find_views_for_element(element, only_on_sheets, view_sheet_map)
    show_views_table(views_info, view_sheet_map, element)


if __name__ == "__main__":
    main()
