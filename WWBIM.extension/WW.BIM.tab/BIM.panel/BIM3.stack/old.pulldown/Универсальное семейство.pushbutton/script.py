# -*- coding: utf-8 -*-
# Маски параметров → подстановка значений и запись в целевые параметры.
# Охват обработки: Выбранные | Текущий вид | Весь проект.

from Autodesk.Revit.DB import (
    Transaction, ElementId, StorageType, FilteredElementCollector
)
from Autodesk.Revit.UI import TaskDialog

# pyRevit API
from pyrevit import forms

uidoc = __revit__.ActiveUIDocument
doc = uidoc.Document

# Соответствия: источник (маска) -> назначение (куда пишем результат)
MASK_TO_TARGET = [
    (u"Маска наименования", u"ADSK_Наименование"),
    (u"Маска марки", u"ADSK_Марка"),
    (u"Маска кода изделия", u"ADSK_Код изделия"),
]

# Допустимые плейсхолдеры (имена параметров, которые можно вставлять в маску)
PLACEHOLDERS = [
    u"ADSK_Размер_Длина",
    u"ADSK_Размер_Ширина",
    u"ADSK_Размер_Высота",
    u"ADSK_Размер_Диаметр",
]

def find_param(el, name, for_write=False):
    """Возвращает параметр с приоритетом: экземпляр -> тип; for_write исключает read-only."""
    if not el or not name:
        return None
    # На экземпляре
    try:
        p = el.LookupParameter(name)
        if p is not None and (not for_write or not p.IsReadOnly):
            return p
    except:
        pass
    # На типе
    try:
        tid = el.GetTypeId()
        if tid and tid != ElementId.InvalidElementId:
            et = el.Document.GetElement(tid)
            if et is not None:
                pt = et.LookupParameter(name)
                if pt is not None and (not for_write or not pt.IsReadOnly):
                    return pt
    except:
        pass
    return None

def get_param_string(el, name):
    """Возвращает строковое значение: для строк AsString, иначе AsValueString."""
    p = find_param(el, name, for_write=False)
    if p is None:
        return u""
    try:
        if p.StorageType == StorageType.String:
            s = p.AsString()
            return s if s is not None else u""
        s = p.AsValueString()
        return s if s is not None else u""
    except:
        return u""

def set_param_string(el, name, value):
    """Пишет строку в параметр (экземпляр/тип). Возвращает True при успехе."""
    p = find_param(el, name, for_write=True)
    if p is None:
        return False
    try:
        if p.StorageType == StorageType.String:
            return p.Set(value)
        else:
            p.SetValueString(value)
            return True
    except:
        return False

def build_from_mask(el, mask_text):
    """Меняет в тексте маски имена параметров на их значения. Не найдено → пустая строка."""
    if not mask_text:
        return u""
    result = mask_text
    for ph in PLACEHOLDERS:
        val = get_param_string(el, ph) or u""
        result = result.replace(ph, val)
    return result

def choose_scope():
    """Окно выбора охвата обработки."""
    options = [u"Выбранные элементы", u"Текущий вид", u"Весь проект"]
    choice = forms.CommandSwitchWindow.show(options, message=u"Что обрабатывать?")
    return choice

def collect_elements(choice):
    """Собирает элементы согласно выбранному охвату."""
    if choice == u"Выбранные элементы":
        sel_ids = list(uidoc.Selection.GetElementIds())
        return [doc.GetElement(eid) for eid in sel_ids if doc.GetElement(eid) is not None]
    elif choice == u"Текущий вид":
        av = doc.ActiveView
        if av is None:
            return []
        return list(FilteredElementCollector(doc, av.Id).WhereElementIsNotElementType())
    else:  # "Весь проект"
        return list(FilteredElementCollector(doc).WhereElementIsNotElementType())

def main():
    choice = choose_scope()
    if not choice:
        return
    elements = collect_elements(choice)
    if not elements:
        TaskDialog.Show(u"Нет элементов", u"Подходящих элементов не найдено для выбранного охвата.")
        return

    total = 0
    updated = 0
    t = Transaction(doc, u"WW.BIM: заполнение по маскам ({})".format(choice))
    t.Start()
    try:
        for el in elements:
            total += 1
            changed = False
            # Обновляем по каждой маске
            for src_name, dst_name in MASK_TO_TARGET:
                mask_text = get_param_string(el, src_name)
                if not mask_text or mask_text.strip() == u"":
                    continue
                built = build_from_mask(el, mask_text)
                if set_param_string(el, dst_name, built):
                    changed = True
            if changed:
                updated += 1
        t.Commit()
    except Exception as ex:
        try:
            t.RollBack()
        except:
            pass
        TaskDialog.Show(u"Ошибка", u"Не удалось выполнить обработку:\n{}".format(ex))
        return

    TaskDialog.Show(
        u"Готово",
        u"Охват: {0}\nОбработано элементов: {1}\nОбновлено: {2}\n\n"
        u"Плейсхолдеры: {3}".format(
            choice, total, updated, u", ".join(PLACEHOLDERS)
        )
    )

if __name__ == '__main__':
    main()