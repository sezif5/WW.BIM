# -*- coding: utf-8 -*-
# pyRevit button: "Заполнить параметры для всех элементов в модели"
# Автор: вы :)  | Требования: pyRevit (Python 3), RevitAPI
__title__  = u"Номер секции\n(Комплект)"
__author__ = "Влад"
__doc__    = u"""Заполняет выбранные параметры для всех элементов в модели"""

from pyrevit import forms, script
from Autodesk.Revit.DB import (
    FilteredElementCollector, Transaction, StorageType
)

doc = __revit__.ActiveUIDocument.Document
output = script.get_output()

# ---- настройки по умолчанию (можете поменять прямо тут) ----
DEFAULT_PARAM_1_NAME = u"ADSK_Номер секции"
DEFAULT_PARAM_1_VALUE = u"МФК"

DEFAULT_PARAM_2_NAME = u"ADSK_Комплект"
DEFAULT_PARAM_2_VALUE = u"ИОС2.3"
# -------------------------------------------------------------

# Быстрый запрос строк с дефолтами.
def ask_str(title, default):
    return forms.ask_for_string(
        default=default,
        prompt=title,
        title="ВИС — заполнение параметров"
    )

p1_name = ask_str(u"Имя параметра 1", DEFAULT_PARAM_1_NAME)
p1_value = ask_str(u"Значение 1", DEFAULT_PARAM_1_VALUE)
p2_name = ask_str(u"Имя параметра 2", DEFAULT_PARAM_2_NAME)
p2_value = ask_str(u"Значение 2", DEFAULT_PARAM_2_VALUE)

# Если пользователь закрыл любое окно -> прерываем
if p1_name is None or p1_value is None or p2_name is None or p2_value is None:
    forms.alert(u"Операция отменена.", title="ВИС — заполнение параметров", warn_icon=True)
    script.exit()

def set_param(elem, pname, pvalue):
    """Пытается записать строковое значение в параметр по имени.
    Возвращает True, если удалось записать.
    """
    if not pname or not pvalue:
        return False

    par = elem.LookupParameter(pname)
    if par is None or par.IsReadOnly:
        return False

    try:
        st = par.StorageType
        if st == StorageType.String:
            par.Set(str(pvalue))
            return True
        elif st == StorageType.Integer:
            # пробуем привести к int
            try:
                par.Set(int(pvalue))
                return True
            except Exception:
                return False
        elif st == StorageType.Double:
            # без единиц — как есть; при необходимости пользователь перезапишет
            try:
                par.Set(float(pvalue))
                return True
            except Exception:
                return False
        elif st == StorageType.ElementId:
            # не поддерживаем автоматическое сопоставление ElementId по строке
            return False
    except Exception:
        return False
    return False

# Сбор всех экземпляров в активном документе
all_elems = FilteredElementCollector(doc).WhereElementIsNotElementType().ToElements()

count1 = 0
count2 = 0

t = Transaction(doc, "ВИС: заполнение параметров для всех элементов")
t.Start()
for el in all_elems:
    try:
        if p1_name and p1_value:
            if set_param(el, p1_name, p1_value):
                count1 += 1
        if p2_name and p2_value:
            if set_param(el, p2_name, p2_value):
                count2 += 1
    except Exception:
        # защищаем транзакцию от случайных падений на редких элементах
        pass
t.Commit()

msg = u"Готово!\n" \
      u"Изменено элементов:\n" \
      u"  • {0} — параметр «{1}» → «{2}»\n" \
      u"  • {3} — параметр «{4}» → «{5}»".format(
          count1, p1_name, p1_value, count2, p2_name, p2_value
      )

output.print_md("### " + msg)
forms.alert(msg, title="ВИС — заполнение параметров", ok=True)
