# -*- coding: utf-8 -*-
__title__  = u"ADSK_Группирование\n(по категориям)"
__author__ = "vlad / you"
__doc__    = u"""Заполняет параметр 'ADSK_Группирование' по словарю
соответствий 'Категория → Группа' для всех элементов инженерных категорий.
Оптимизировано: обновление только при изменении, учёт типовых параметров,
коммит по категориям, подавление предупреждений.

Дополнено: если в 'ADSK_Наименование' встречается 'вентилят...' или 'шумоглуш...',
то присваивается значение группирования как у категории 'Механическое оборудование'.
"""

from pyrevit import revit, script
import Autodesk.Revit.DB as DB
from Autodesk.Revit.DB import BuiltInCategory as BIC

doc = revit.doc
out = script.get_output()
out.close_others(all_open_outputs=True)
out.set_width(1100)

PARAM_NAME = u"ADSK_Группирование"
NAME_PARAM = u"ADSK_Наименование"

# ---- Мэппинг «Категория → значение»
CAT2GROUP = {
    BIC.OST_DuctAccessory       : u"2.Запорно-регулирующая арматура",
    BIC.OST_PipeAccessory       : u"2.Запорно-регулирующая арматура",
    BIC.OST_DuctTerminal        : u"4.Другие элементы систем ",
    BIC.OST_PlumbingFixtures    : u"4.Другие элементы систем",
    BIC.OST_PipeFitting         : u"4.Другие элементы систем",
    BIC.OST_Sprinklers          : u"1.Оборудование",
    BIC.OST_DuctFitting         : u"4.Другие элементы систем",
    BIC.OST_MechanicalEquipment : u"1.Оборудование",
    BIC.OST_DuctCurves          : u"3.Воздуховоды",
    BIC.OST_GenericModel       : u"5.Обобщенные модели",
    BIC.OST_FlexDuctCurves      : u"3.Воздуховоды",
    BIC.OST_PipeCurves          : u"3.Трубы",
    BIC.OST_FlexPipeCurves      : u"3.Трубопроводы",
    BIC.OST_CableTray           : u"5.Материалы",
    BIC.OST_PipeInsulations     : u"5.Материалы изоляции",
    BIC.OST_DuctInsulations     : u"5.Материалы изоляции",
    # при необходимости добавь:
    # BIC.OST_CableTrayFitting  : u"...",
    # BIC.OST_Conduit           : u"...",
    # BIC.OST_ConduitFitting    : u"...",
}

# ---- Ключевые слова для правила "вентилятор / шумоглушитель"
# Используем основы слов, чтобы покрыть формы: вентилятор/вентиляторы, шумоглушитель/шумоглушители
KW_NAME_STEMS = (u"вентилят", u"шумоглуш")

# ---------- helpers ----------
def _as_text(p):
    if not p or not p.HasValue:
        return None
    try:
        return p.AsString() or p.AsValueString()
    except Exception:
        return None

def _set_text(p, val):
    try:
        p.Set(val)
        return True
    except Exception:
        return False

def _get_param_text(element, param_name):
    """Возвращает текст параметра element[param_name]; если его нет на экземпляре,
    пробует взять из типа."""
    # instance
    p = getattr(element, "LookupParameter", None)
    if callable(p):
        val = _as_text(element.LookupParameter(param_name))
        if val:
            return val
    # type
    try:
        tid = element.GetTypeId()
        if tid:
            typ = doc.GetElement(tid)
            if typ:
                return _as_text(typ.LookupParameter(param_name))
    except Exception:
        pass
    return None

class _SwallowWarnings(DB.IFailuresPreprocessor):
    """Удаляем предупреждения, чтобы транзакция не поднимала диалоги."""
    def PreprocessFailures(self, accessor):
        for f in list(accessor.GetFailureMessages()):
            if f.GetSeverity() == DB.FailureSeverity.Warning:
                accessor.DeleteWarning(f)
        return DB.FailureProcessingResult.Continue

def _begin_tx(name):
    t = DB.Transaction(doc, name)
    t.Start()
    fho = t.GetFailureHandlingOptions()
    fho.SetFailuresPreprocessor(_SwallowWarnings())
    t.SetFailureHandlingOptions(fho)
    return t

# ---- Правило переопределения значения по имени
MECH_GROUP_VALUE = CAT2GROUP.get(BIC.OST_MechanicalEquipment, u"1.Оборудование")

def _group_value_for_element(el, default_value):
    """Если в ADSK_Наименование встречаются нужные слова — вернуть значение как у механического оборудования,
    иначе вернуть default_value."""
    name = _get_param_text(el, NAME_PARAM)
    if name:
        lname = name.lower()
        if any(stem in lname for stem in KW_NAME_STEMS):
            return MECH_GROUP_VALUE
    return default_value

# ---------- main ----------
def main():
    total_updated = 0
    failures_rows = []
    summary_rows  = []

    # кэш для типовых параметров: чтобы писать один раз на тип
    updated_types = set()  # ElementId.IntegerValue

    for bic, group_value in CAT2GROUP.items():
        # сбор элементов категории
        col = (DB.FilteredElementCollector(doc)
               .OfCategory(bic)
               .WhereElementIsNotElementType())

        count = col.GetElementCount()
        summary_updated = 0
        if count == 0:
            summary_rows.append([bic.ToString(), 0, group_value])
            continue

        # транзакция на категорию
        t = _begin_tx(u"ADSK_Группирование: {}".format(bic.ToString()))
        try:
            for idx, el in enumerate(col, 1):
                # определяем целевое значение с учётом правила по имени
                target_value = _group_value_for_element(el, group_value)

                # 1) пробуем инстанс-параметр
                p = el.LookupParameter(PARAM_NAME)
                if p and not p.IsReadOnly:
                    cur = _as_text(p) or u""
                    if cur != target_value:
                        if _set_text(p, target_value):
                            summary_updated += 1
                        else:
                            failures_rows.append([out.linkify(el.Id), el.Category.Name, u"не удалось записать (instance)"])
                    if idx % 200 == 0:
                        out.update_progress(idx, count)
                    continue

                # 2) типовой параметр (обновляем один раз на тип)
                try:
                    tid = el.GetTypeId()
                except Exception:
                    tid = None
                if tid and tid.IntegerValue not in updated_types:
                    typ = doc.GetElement(tid)
                    if typ:
                        pt = typ.LookupParameter(PARAM_NAME)
                        if pt and not pt.IsReadOnly:
                            cur = _as_text(pt) or u""
                            if cur != target_value:
                                if _set_text(pt, target_value):
                                    summary_updated += 1
                                    updated_types.add(tid.IntegerValue)
                                else:
                                    failures_rows.append([out.linkify(el.Id), el.Category.Name, u"не удалось записать (type)"])
                            else:
                                updated_types.add(tid.IntegerValue)
                if idx % 200 == 0:
                    out.update_progress(idx, count)
        finally:
            t.Commit()

        total_updated += summary_updated
        summary_rows.append([bic.ToString(), summary_updated, group_value])
        out.update_progress(count, count)

    # -------- вывод
    out.print_md(u"### Итог")
    out.print_md(u"- Обновлено значений: **{}**".format(total_updated))
    out.print_md(u"- Параметр: **{}**".format(PARAM_NAME))
    out.print_md(u"—")
    out.print_md(u"### Сводка по категориям")
    out.print_table(summary_rows, [u"Категория", u"Обновлено", u"Группа (значение)"])

    if failures_rows:
        out.print_md(u"### Не удалось записать параметр у некоторых элементов")
        out.print_table(failures_rows, [u"Элемент", u"Категория", u"Причина"])
    else:
        out.print_md(u"Готово. Проблем не обнаружено.")

if __name__ == "__main__":
    main()
