# -*- coding: utf-8 -*-
from __future__ import print_function

__title__ = u"Размеры\nна осях"
__doc__ = u"""Проставляет цепочки размеров на осях вида:
- Расстояния между каждой соседней парой осей
- Один общий размер между крайними осями"""

import clr
clr.AddReference("RevitAPI")
clr.AddReference("RevitAPIUI")

from Autodesk.Revit.DB import (
    FilteredElementCollector, Grid, Line, XYZ, ReferenceArray, Reference,
    ViewPlan, ViewSection
)
from Autodesk.Revit.UI import TaskDialog
from pyrevit import revit, forms


def get_grid_direction(grid):
    u"""Определяет направление оси (вертикальная или горизонтальная)"""
    curve = grid.Curve
    start = curve.GetEndPoint(0)
    end = curve.GetEndPoint(1)

    dx = abs(end.X - start.X)
    dy = abs(end.Y - start.Y)

    if dx > dy:
        return u"horizontal"
    else:
        return u"vertical"


def get_grid_position(grid):
    u"""Получает координату оси для сортировки"""
    curve = grid.Curve
    start = curve.GetEndPoint(0)
    end = curve.GetEndPoint(1)
    mid = (start + end) * 0.5
    direction = get_grid_direction(grid)

    if direction == u"horizontal":
        return mid.Y
    else:
        return mid.X


def get_common_coordinate(grids, direction):
    u"""Находит общую координату для размерной линии"""
    coords = []

    for grid in grids:
        curve = grid.Curve
        start = curve.GetEndPoint(0)
        end = curve.GetEndPoint(1)

        if direction == u"horizontal":
            # Для горизонтальных осей берем минимальный X
            coords.append(min(start.X, end.X))
        else:
            # Для вертикальных осей берем минимальный Y
            coords.append(min(start.Y, end.Y))

    return min(coords) if coords else 0


def create_dimension_line(doc, view, references, line):
    u"""Создает размерную линию"""
    try:
        dimension = doc.Create.NewDimension(view, line, references)
        return dimension
    except Exception as e:
        print(u"Ошибка создания размера: {}".format(e))
        return None


def place_dimensions_on_grids(doc, view, direction, offset1=5, offset2=10):
    u"""Размещает размеры на осях заданного направления"""
    # Собираем все оси на виде
    grids = FilteredElementCollector(doc, view.Id)\
        .OfClass(Grid)\
        .ToElements()

    if not grids:
        return 0

    # Фильтруем оси по направлению
    filtered_grids = [g for g in grids if get_grid_direction(g) == direction]

    if len(filtered_grids) < 2:
        return 0

    # Сортируем оси по позиции
    sorted_grids = sorted(filtered_grids, key=lambda g: get_grid_position(g))

    # Получаем общую базовую координату
    base_coord = get_common_coordinate(sorted_grids, direction)

    dimensions_created = 0

    # Создаем цепочный размер между всеми осями
    references_chain = ReferenceArray()
    for grid in sorted_grids:
        references_chain.Append(Reference(grid))

    first_pos = get_grid_position(sorted_grids[0])
    last_pos = get_grid_position(sorted_grids[-1])

    # Создаем размерную линию для цепочки
    if direction == u"vertical":
        point1 = XYZ(first_pos, base_coord - offset1, 0)
        point2 = XYZ(last_pos, base_coord - offset1, 0)
    else:
        point1 = XYZ(base_coord - offset1, first_pos, 0)
        point2 = XYZ(base_coord - offset1, last_pos, 0)

    dim_line_chain = Line.CreateBound(point1, point2)

    if create_dimension_line(doc, view, references_chain, dim_line_chain):
        dimensions_created += 1

    # Создаем общий размер между крайними осями
    if len(sorted_grids) >= 2:
        references_total = ReferenceArray()
        references_total.Append(Reference(sorted_grids[0]))
        references_total.Append(Reference(sorted_grids[-1]))

        if direction == u"vertical":
            point1 = XYZ(first_pos, base_coord - offset2, 0)
            point2 = XYZ(last_pos, base_coord - offset2, 0)
        else:
            point1 = XYZ(base_coord - offset2, first_pos, 0)
            point2 = XYZ(base_coord - offset2, last_pos, 0)

        dim_line_total = Line.CreateBound(point1, point2)

        if create_dimension_line(doc, view, references_total, dim_line_total):
            dimensions_created += 1

    return dimensions_created


# Основной код
doc = revit.doc
uidoc = revit.uidoc
view = doc.ActiveView

# Проверяем, что текущий вид подходит для размеров
if not isinstance(view, ViewPlan) and not isinstance(view, ViewSection):
    TaskDialog.Show(u"Ошибка", u"Скрипт работает только на планах и разрезах")
else:
    # Спрашиваем направление осей
    options = [u"Вертикальные оси", u"Горизонтальные оси", u"Обе"]
    selected = forms.CommandSwitchWindow.show(
        options,
        message=u"Выберите направление осей для простановки размеров:"
    )

    if selected:
        with revit.Transaction(u"Размеры на осях"):
            total_dimensions = 0

            if selected == u"Вертикальные оси" or selected == u"Обе":
                total_dimensions += place_dimensions_on_grids(doc, view, u"vertical")

            if selected == u"Горизонтальные оси" or selected == u"Обе":
                total_dimensions += place_dimensions_on_grids(doc, view, u"horizontal")

            if total_dimensions > 0:
                TaskDialog.Show(u"Готово",
                    u"Создано размеров: {}".format(total_dimensions))
            else:
                TaskDialog.Show(u"Внимание",
                    u"Не удалось создать размеры. Проверьте наличие осей на виде.")
