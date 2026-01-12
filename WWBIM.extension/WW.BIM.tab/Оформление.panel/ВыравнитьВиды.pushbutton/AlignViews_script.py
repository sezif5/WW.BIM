# -*- coding: utf-8 -*-
__title__ = 'Выравнить\nЭлементы'
__author__ = 'WW.BIM'

from Autodesk.Revit.DB import *
from Autodesk.Revit.UI.Selection import ObjectType
from pyrevit import revit, DB, UI

doc = revit.doc
uidoc = revit.uidoc


class SheetElementFilter(UI.Selection.ISelectionFilter):
    """Фильтр для выбора элементов на листах"""
    def AllowElement(self, element):
        # Виды (обычные, легенды, чертежные виды)
        if isinstance(element, Viewport):
            return True
        # Спецификации
        if isinstance(element, ScheduleSheetInstance):
            return True
        # Текстовые заметки
        if isinstance(element, TextNote):
            return True
        # Другие аннотационные элементы
        if hasattr(element, 'Location') or hasattr(element, 'BoundingBox'):
            owner = element.OwnerViewId
            owner_view = doc.GetElement(owner)
            # Проверяем, что элемент на листе
            if owner_view and isinstance(owner_view, ViewSheet):
                return True
        return False

    def AllowReference(self, reference, position):
        return False


def get_element_center(element):
    """Получить центр элемента на листе"""
    # Viewport (виды, легенды, чертежные виды)
    if isinstance(element, Viewport):
        outline = element.GetBoxOutline()
        min_point = outline.MinimumPoint
        max_point = outline.MaximumPoint
        return XYZ(
            (min_point.X + max_point.X) / 2,
            (min_point.Y + max_point.Y) / 2,
            0
        )

    # Спецификации
    elif isinstance(element, ScheduleSheetInstance):
        bbox = element.get_BoundingBox(doc.GetElement(element.OwnerViewId))
        if bbox:
            min_point = bbox.Min
            max_point = bbox.Max
            return XYZ(
                (min_point.X + max_point.X) / 2,
                (min_point.Y + max_point.Y) / 2,
                0
            )

    # Текстовые заметки
    elif isinstance(element, TextNote):
        return element.Coord

    # Другие элементы с Location
    elif hasattr(element, 'Location') and element.Location:
        if isinstance(element.Location, LocationPoint):
            return element.Location.Point
        elif isinstance(element.Location, LocationCurve):
            curve = element.Location.Curve
            return curve.Evaluate(0.5, True)

    # Если ничего не подошло, используем BoundingBox
    owner = element.OwnerViewId
    bbox = element.get_BoundingBox(doc.GetElement(owner))
    if bbox:
        min_point = bbox.Min
        max_point = bbox.Max
        return XYZ(
            (min_point.X + max_point.X) / 2,
            (min_point.Y + max_point.Y) / 2,
            0
        )

    return None


def move_element(element, offset):
    """Переместить элемент на указанное смещение"""
    # Viewport
    if isinstance(element, Viewport):
        element.SetBoxCenter(element.GetBoxCenter() + offset)

    # Спецификации
    elif isinstance(element, ScheduleSheetInstance):
        current_point = element.Point
        element.Point = current_point + offset

    # Текстовые заметки
    elif isinstance(element, TextNote):
        element.Coord = element.Coord + offset

    # Элементы с Location
    elif hasattr(element, 'Location') and element.Location:
        if isinstance(element.Location, LocationPoint):
            element.Location.Move(offset)
        elif isinstance(element.Location, LocationCurve):
            element.Location.Move(offset)

    else:
        raise Exception("Не удалось переместить элемент типа {}".format(type(element).__name__))


def main():
    try:
        # Выбор базового элемента
        base_ref = uidoc.Selection.PickObject(
            ObjectType.Element,
            SheetElementFilter(),
            "Выберите базовый элемент на листе"
        )
        base_element = doc.GetElement(base_ref.ElementId)

        if not base_element:
            return

        base_center = get_element_center(base_element)
        if not base_center:
            return

        # Выбор элементов для выравнивания
        target_refs = uidoc.Selection.PickObjects(
            ObjectType.Element,
            SheetElementFilter(),
            "Выберите элементы для выравнивания. Finish для завершения."
        )

        if not target_refs:
            return

        # Выравнивание элементов
        with revit.Transaction("Выравнивание элементов"):
            for ref in target_refs:
                element = doc.GetElement(ref.ElementId)

                # Пропускаем базовый элемент
                if element.Id == base_element.Id:
                    continue

                current_center = get_element_center(element)
                if not current_center:
                    continue

                # Вычисляем смещение
                offset = XYZ(
                    base_center.X - current_center.X,
                    base_center.Y - current_center.Y,
                    0
                )

                # Перемещаем элемент
                try:
                    move_element(element, offset)
                except:
                    pass

    except Exception as e:
        if "cancelled" not in str(e).lower() and "отменено" not in str(e).lower():
            import traceback
            traceback.print_exc()


if __name__ == '__main__':
    main()
