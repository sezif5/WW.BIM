# -*- coding: utf-8 -*-
__title__  = u"ФСК_Описание"
__author__ = u"vlad / you"
__doc__    = u"""Собирает ФСК_Описание для инженерных категорий.
Вложенные НЕ учитываются для: Сплинклеров, Арматуры воздуховодов, Арматуры трубопроводов, Соединительных деталей.
Для Арматуры/Соединительных деталей труб добавляет «Размер» (мм), если в ADSK_Наименование есть дюймы: 1", 1/2", 1 1/2", 1/2''.
Исправлено: размер теперь добавляется и для ВЛОЖЕННЫХ элементов (PipeAccessory/PipeFitting), плюс «план Б» — поиск размера у подкомпонентов без оглядки на запрет вложенных.
"""

import re
from collections import OrderedDict
from pyrevit import script, coreutils
from Autodesk.Revit.DB import (
    Transaction, FilteredElementCollector, ElementId,
    BuiltInCategory as BIC, BuiltInParameter, StorageType, UnitUtils
)

# единицы (старые/новые версии)
try:
    from Autodesk.Revit.DB import UnitTypeId
    _MM = UnitTypeId.Millimeters
except Exception:
    from Autodesk.Revit.DB import DisplayUnitType
    _MM = DisplayUnitType.DUT_MILLIMETERS

out = script.get_output()
out.close_others(all_open_outputs=True)
doc = __revit__.ActiveUIDocument.Document

# ---- инженерные категории
MEP_BICS = [
    BIC.OST_DuctAccessory,
    BIC.OST_PipeAccessory,
    BIC.OST_PipeFitting,          # соединительные детали
    BIC.OST_DuctTerminal,
    BIC.OST_PlumbingFixtures,
    BIC.OST_DuctFitting,
    BIC.OST_FlexPipeCurves,
    BIC.OST_FlexDuctCurves,
    BIC.OST_MechanicalEquipment,
    BIC.OST_DuctCurves,
    BIC.OST_PipeCurves,
    BIC.OST_PipeInsulations,
    BIC.OST_DuctInsulations,
    BIC.OST_LightingDevices,
    BIC.OST_CableTray,
    BIC.OST_Conduit,
    BIC.OST_LightingFixtures,
    BIC.OST_StructConnections,
    BIC.OST_CableTrayFitting,
    BIC.OST_ConduitFitting,
    BIC.OST_ElectricalFixtures,
    BIC.OST_ElectricalEquipment,
    BIC.OST_FireAlarmDevices,
    BIC.OST_DataDevices,
    BIC.OST_GenericModel,
    BIC.OST_CommunicationDevices,
    BIC.OST_NurseCallDevices,
    BIC.OST_Sprinklers,
]

# ---- где НЕ учитываем вложенные
SKIP_NESTED_FOR = {
    int(BIC.OST_Sprinklers),
    int(BIC.OST_DuctAccessory),   # арматура воздуховодов
    int(BIC.OST_PipeAccessory),   # арматура трубопроводов
    int(BIC.OST_PipeFitting),     # соединительные детали трубопроводов
}

# ---- где потенциально добавляем «Размер»
ADD_SIZE_FOR = {int(BIC.OST_PipeAccessory), int(BIC.OST_PipeFitting)}

# ---- имена параметров
DESC_PARAM = u"ФСК_Описание"
P_NAME     = u"ADSK_Наименование"
P_QTY      = u"ADSK_Количество"
P_CODE     = u"ADSK_Код изделия"
P_MFR      = u"ADSK_Завод-изготовитель"
P_MARK     = u"ADSK_Марка"

# ======== ДЮЙМЫ В НАЗВАНИИ ========
_INCH_MARK = u'(?:["”″]|\'\')'
_RX_INCH_INT   = re.compile(u'(?<!\\d)[1-9]\\d*' + _INCH_MARK, re.UNICODE)
_RX_INCH_FRAC  = re.compile(u'(?<!\\d)[1-9]\\d*/\\d+' + _INCH_MARK, re.UNICODE)
_RX_INCH_MIXED = re.compile(u'(?<!\\d)[1-9]\\d*\\s+[1-9]\\d*/\\d+' + _INCH_MARK, re.UNICODE)

def _bic_of(elem):
    try:
        return elem.Category and elem.Category.Id.IntegerValue
    except Exception:
        return None

def _as_text(p):
    if not p or not p.HasValue:
        return None
    try:
        return p.AsString() if p.StorageType == StorageType.String else p.AsValueString()
    except Exception:
        return None

def _get_param_text(elem, pname):
    if not elem:
        return None
    v = _as_text(elem.LookupParameter(pname))
    if v:
        return v
    try:
        tid = elem.GetTypeId()
        if tid and tid != ElementId.InvalidElementId:
            et = doc.GetElement(tid)
            return _as_text(et.LookupParameter(pname)) if et else None
    except Exception:
        pass
    return None

def _fmt_mm(val_mm):
    if val_mm is None:
        return None
    x = round(float(val_mm), 1)
    if abs(x - int(x)) < 1e-6:
        return u"{} мм".format(int(x))
    return u"{} мм".format(x)

def _get_size_from_connectors(elem):
    diams_ft = []
    try:
        mep = getattr(elem, 'MEPModel', None)
        cm  = mep and mep.ConnectorManager
        if cm:
            for c in cm.Connectors:
                try:
                    dft = getattr(c, 'Diameter', 0.0)
                    if dft and dft > 0:
                        diams_ft.append(dft)
                except Exception:
                    pass
    except Exception:
        pass
    if not diams_ft:
        return None
    diams_mm = [UnitUtils.ConvertFromInternalUnits(d, _MM) for d in diams_ft]
    uniq = []
    for d in sorted(diams_mm):
        if not uniq or abs(d - uniq[-1]) > 0.5:
            uniq.append(d)
    if len(uniq) == 1:
        return _fmt_mm(uniq[0])
    else:
        return u"{} – {}".format(_fmt_mm(uniq[0]), _fmt_mm(uniq[-1]))

def _get_size_text(elem):
    # системный параметр
    s = _as_text(elem.get_Parameter(BuiltInParameter.RBS_CALCULATED_SIZE))
    if s:
        s = s.strip()
        low = s.lower().replace(" ", "")
        return s if low.endswith((u"мм","mm")) else (s + u" мм")
    # имена на экземпляре/типе
    for name in (u"Общий размер", u"Размер", "Overall Size", "Size"):
        s = _get_param_text(elem, name)
        if s:
            s = s.strip()
            if not s:
                continue
            low = s.lower().replace(" ", "")
            return s if low.endswith((u"мм","mm")) else (s + u" мм")
    # коннекторы
    return _get_size_from_connectors(elem)

def _has_inches(text):
    if not text:
        return False
    nm = text.strip()
    return bool(_RX_INCH_INT.search(nm) or _RX_INCH_FRAC.search(nm) or _RX_INCH_MIXED.search(nm))

def _get_subcomponents(elem):
    if _bic_of(elem) in SKIP_NESTED_FOR:
        return []
    res = []
    try:
        for sid in elem.GetSubComponentIds():
            sub = doc.GetElement(sid)
            if sub:
                res.append(sub)
    except Exception:
        pass
    return res

def _iter_nested_any(elem):
    try:
        for sid in elem.GetSubComponentIds():
            sub = doc.GetElement(sid)
            if sub:
                yield sub
    except Exception:
        return

# --- НОВОЕ: «нулевое» или пустое количество -> "!Не учитывается"
def _qty_is_zero_or_missing(qty_text):  # <<< добавлено
    if qty_text is None:
        return True
    s = qty_text.strip()
    if not s:
        return True
    s_nospace = s.replace(" ", "")
    # частые текстовые представления нуля
    if s_nospace in (u"0", u"0.0", u"0,0", u"0.00", u"0,00", u"0.000", u"0,000"):
        return True
    # попытка извлечь число из строки (вдруг есть единицы измерения)
    ss = s_nospace.replace(",", ".")
    m = re.search(r'-?\d+(?:\.\d+)?', ss)
    if m:
        try:
            return abs(float(m.group(0))) < 1e-12
        except Exception:
            pass
    return False
# --- конец нового блока

def _compose_description(elem):
    name = _get_param_text(elem, P_NAME)
    if name and u"!Не учитывать" in name:
        return None

    qty = _get_param_text(elem, P_QTY)
    # БЫЛО: if qty and qty.strip() == u"0": return None
    # СТАЛО:
    if _qty_is_zero_or_missing(qty):     # <<< изменено
        return u"!Не учитывается"         # <<< изменено

    code = _get_param_text(elem, P_CODE)
    mfr  = _get_param_text(elem, P_MFR)
    mark = _get_param_text(elem, P_MARK)

    parts = [x for x in [name, code, mark, mfr] if x]
    size_appended = False

    # ---- размер для ГЛАВНОГО элемента
    if _bic_of(elem) in ADD_SIZE_FOR and _has_inches(name):
        sz = _get_size_text(elem)
        if sz:
            parts.append(sz)
            size_appended = True

    # ---- вложенные (если разрешено текущей категорией)
    for sub in _get_subcomponents(elem):
        n_name = _get_param_text(sub, P_NAME)
        if n_name and u"!Не учитывать" in n_name:
            continue
        n_qty = _get_param_text(sub, P_QTY)
        if n_qty and n_qty.strip() != u"1":
            continue
        n_code = _get_param_text(sub, P_CODE)
        n_mfr  = _get_param_text(sub, P_MFR)
        n_mark = _get_param_text(sub, P_MARK)

        n_desc_parts = [n_name, n_code, n_mark, n_mfr]

        if _bic_of(sub) in ADD_SIZE_FOR and _has_inches(n_name):
            n_sz = _get_size_text(sub)
            if n_sz:
                n_desc_parts.append(n_sz)
                size_appended = True

        desc = u" ".join(x for x in n_desc_parts if x).strip()
        if desc:
            parts.append(desc)

    # ---- «План Б» для размера
    if not size_appended:
        for sub in _iter_nested_any(elem):
            if _bic_of(sub) in ADD_SIZE_FOR:
                sub_name = _get_param_text(sub, P_NAME)
                if _has_inches(sub_name):
                    n_sz = _get_size_text(sub)
                    if n_sz:
                        parts.append(n_sz)
                        size_appended = True
                        break

    return u" ".join(parts) if parts else u""

def _collect_mep_elements():
    uniq = OrderedDict()
    for bic in MEP_BICS:
        try:
            for e in FilteredElementCollector(doc).OfCategory(bic).WhereElementIsNotElementType():
                uniq[e.Id.IntegerValue] = e
        except Exception:
            pass
    return list(uniq.values())

def main():
    elems = _collect_mep_elements()
    if not elems:
        out.print_md(u":information_source: В инженерных категориях элементов не найдено.")
        return

    out.print_md(u"### ФСК_Описание: обработка {} элементов".format(len(elems)))
    t = coreutils.Timer()
    updated = skipped = readonly = 0

    tr = Transaction(doc, u"ФСК_Описание (MEP)")
    tr.Start()
    try:
        for i, e in enumerate(elems, 1):
            desc = _compose_description(e)
            if desc is None:
                skipped += 1
                continue
            p = e.LookupParameter(DESC_PARAM)
            if p and not p.IsReadOnly:
                try:
                    p.Set(desc)
                    updated += 1
                except Exception:
                    readonly += 1
            else:
                readonly += 1
            if i % 50 == 0:
                out.update_progress(i, len(elems))
    finally:
        tr.Commit()

    out.update_progress(len(elems), len(elems))
    out.print_md(u"- Обновлено: **{}**".format(updated))
    out.print_md(u"- Пропущено по условиям: **{}**".format(skipped))
    out.print_md(u"- Параметр недоступен/только чтение: **{}**".format(readonly))
    out.print_md(u"_Готово за {} c._".format(int(t.get_time())))

if __name__ == "__main__":
    main()
