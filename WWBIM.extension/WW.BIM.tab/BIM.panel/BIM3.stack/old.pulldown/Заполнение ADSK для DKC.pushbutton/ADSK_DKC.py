# -*- coding: utf-8 -*-
# pyRevit (IronPython) — копирование DKC_* -> ADSK_* для лотков/фитингов/прогонов и Механического оборудования
from pyrevit import revit, DB, forms

doc = revit.doc

# --- Пары параметров (источник -> цель) ---
# Для DKC_ДлинаФакт предусмотрен фолбэк на DKC_Количество
PARAM_MAP = [
    ("DKC_Наименование",       "ADSK_Наименование"),
    ("DKC_Марка",               "ADSK_Марка"),
    ("DKC_ДлинаФакт",           "ADSK_Количество"),   # если нет DKC_ДлинаФакт -> берём DKC_Количество
    ("DKC_Код изделия",         "ADSK_Код изделия"),
    ("DKC_Завод-изготовитель",  "ADSK_Завод-изготовитель"),
]

# --- Категории: лотки, фитинги, (прогоны), мех.оборудование ---
CATEGORIES = [
    DB.BuiltInCategory.OST_CableTray,
    DB.BuiltInCategory.OST_CableTrayFitting,
    DB.BuiltInCategory.OST_MechanicalEquipment,        # <<< добавлено
]
# В новых версиях Revit бывают прогоны лотков
try:
    CATEGORIES.append(DB.BuiltInCategory.OST_CableTrayRun)
except Exception:
    pass

# --- Утилиты ---
def get_param_instance_then_type(el, name):
    """Ищем параметр сначала у экземпляра, затем у типа."""
    try:
        p = el.LookupParameter(name)
        if p: return p
    except Exception:
        pass
    try:
        tid = el.GetTypeId() if hasattr(el, "GetTypeId") else None
        if tid and tid != DB.ElementId.InvalidElementId:
            typ = doc.GetElement(tid)
            if typ:
                return typ.LookupParameter(name)
    except Exception:
        pass
    return None

def parse_float(s):
    try:
        s = (s or "").strip().replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def parse_int(s):
    try:
        s = (s or "").strip().replace(" ", "")
        s_norm = s.replace(",", ".")
        if "." in s_norm:
            return int(round(float(s_norm)))
        return int(s)
    except Exception:
        return None

def set_value_simple(src, tgt):
    """Прямое копирование с мягкими конверсиями. Возвращает (ok, reason|None)."""
    if tgt.IsReadOnly:
        return False, u"цель read-only"

    st = src.StorageType
    tt = tgt.StorageType
    try:
        if tt == DB.StorageType.String:
            if st == DB.StorageType.String:
                tgt.Set(src.AsString()); return True, None
            elif st == DB.StorageType.Double:
                tgt.Set(str(src.AsDouble())); return True, None
            elif st == DB.StorageType.Integer:
                tgt.Set(str(src.AsInteger())); return True, None
            elif st == DB.StorageType.ElementId:
                tgt.Set(str(src.AsElementId().IntegerValue)); return True, None
            else:
                return False, u"неподдерживаемый тип источника"

        if tt == DB.StorageType.Double:
            if st == DB.StorageType.Double:
                tgt.Set(src.AsDouble()); return True, None
            if st == DB.StorageType.Integer:
                tgt.Set(float(src.AsInteger())); return True, None
            if st == DB.StorageType.String:
                f = parse_float(src.AsString())
                if f is not None:
                    tgt.Set(f); return True, None
                return False, u"некорректное число в строке"
            return False, u"несовпадение типов {}→Double".format(st)

        if tt == DB.StorageType.Integer:
            if st == DB.StorageType.Integer:
                tgt.Set(src.AsInteger()); return True, None
            if st == DB.StorageType.Double:
                tgt.Set(int(round(src.AsDouble()))); return True, None
            if st == DB.StorageType.String:
                iv = parse_int(src.AsString())
                if iv is not None:
                    tgt.Set(iv); return True, None
                return False, u"некорректное целое в строке"
            return False, u"несовпадение типов {}→Integer".format(st)

        if tt == DB.StorageType.ElementId and st == DB.StorageType.ElementId:
            tgt.Set(src.AsElementId()); return True, None

        return False, u"несовпадение типов {}→{}".format(st, tt)
    except Exception as ex:
        return False, u"ошибка записи: {}".format(ex)

def collect_elements():
    elems = []
    for bic in CATEGORIES:
        col = DB.FilteredElementCollector(doc).OfCategory(bic).WhereElementIsNotElementType()
        elems.extend(list(col))
    return elems

# --- Основной код ---
def main():
    elements = collect_elements()
    if not elements:
        forms.alert(u"В модели нет подходящих элементов (лотки/фитинги/прогоны/мех.оборудование).", exitscript=True)

    total = len(elements)
    ok_any = 0
    reasons_counter = {}

    t = DB.Transaction(doc, u"DKC → ADSK: копирование")
    t.Start()
    try:
        with forms.ProgressBar(title=u"Копирование DKC → ADSK…", cancellable=True, step=1) as pb:
            for i, el in enumerate(elements, 1):
                if pb.cancelled:
                    t.RollBack()
                    forms.alert(u"Отменено пользователем.", exitscript=True)

                element_ok = False
                for src_name, tgt_name in PARAM_MAP:
                    # Фолбэк для длины: если нет DKC_ДлинаФакт, берём DKC_Количество
                    src = get_param_instance_then_type(el, src_name)
                    if src_name == "DKC_ДлинаФакт" and not src:
                        src = get_param_instance_then_type(el, "DKC_Количество")

                    tgt = get_param_instance_then_type(el, tgt_name)

                    if not src:
                        key = u"нет источника '{}'".format(
                            src_name if src_name != "DKC_ДлинаФакт" else "DKC_ДлинаФакт|DKC_Количество"
                        )
                        reasons_counter[key] = reasons_counter.get(key, 0) + 1
                        continue
                    if not tgt:
                        key = u"нет цели '{}'".format(tgt_name)
                        reasons_counter[key] = reasons_counter.get(key, 0) + 1
                        continue

                    ok, reason = set_value_simple(src, tgt)
                    if ok:
                        element_ok = True
                    else:
                        key = reason or u"неизвестная причина"
                        reasons_counter[key] = reasons_counter.get(key, 0) + 1

                if element_ok:
                    ok_any += 1

                pb.update_progress(i, total)
        t.Commit()
    except Exception as ex:
        t.RollBack()
        forms.alert(u"Ошибка транзакции: {}".format(ex), exitscript=True)

    fail = total - ok_any
    print(u"DKC → ADSK: итог")
    print(u"- Всего элементов: {}".format(total))
    print(u"- Успешно обработано (хотя бы один параметр): {}".format(ok_any))
    print(u"- Не обработано: {}".format(fail))

    if reasons_counter:
        print(u"Краткая сводка причин (топ-5):")
        top = sorted(reasons_counter.items(), key=lambda kv: kv[1], reverse=True)[:5]
        for r, c in top:
            print(u"- {} — {} шт.".format(r, c))

if __name__ == "__main__":
    main()
