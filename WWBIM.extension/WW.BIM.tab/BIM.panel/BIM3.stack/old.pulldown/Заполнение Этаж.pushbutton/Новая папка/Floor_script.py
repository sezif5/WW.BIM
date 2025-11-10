# -*- coding: utf-8 -*-
__title__  = "Этаж"
__author__ = "Vlad"
__doc__    = """Заполняет параметр ADSK_Этаж для элементов на основании Уровней в файле.
Добавлено: допускаемое смещение вниз от уровня (по умолчанию 100 мм).
Добавлено: поддержка общестроительных категорий (Архитектура и Конструктив).
"""

from pyrevit import revit, script, forms
import Autodesk.Revit.DB as DB
from Autodesk.Revit.DB import (
    BuiltInCategory, FilteredElementCollector, BuiltInParameter,
    UnitUtils, UnitTypeId, StorageType
)

doc = revit.doc
out = script.get_output()
out.close_others(all_open_outputs=True)

PARAM_NAME = u"ADSK_Этаж"

# ==== НОВОЕ: допускаемое смещение вниз от уровня ====
# Если элемент ниже уровня не более, чем на это значение, присваиваем верхний уровень.
FLOOR_TOLERANCE_MM = 100.0
FLOOR_TOLERANCE_INTERNAL = UnitUtils.ConvertToInternalUnits(FLOOR_TOLERANCE_MM, UnitTypeId.Millimeters)
# =====================================================

# Линейные инженерные системы
LINEAR_CATS = [
    BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_DuctCurves,
    BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_FlexDuctCurves,
    BuiltInCategory.OST_PipeInsulations,
    BuiltInCategory.OST_CableTray,
    BuiltInCategory.OST_DuctInsulations,
]

# Вставляемые инженерные семейства
NONLINEAR_CATS = [
    BuiltInCategory.OST_DuctAccessory,
    BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_DuctTerminal,
    BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_Sprinklers,
    BuiltInCategory.OST_DuctFitting,
    BuiltInCategory.OST_GenericModel,
    BuiltInCategory.OST_LightingDevices,
    BuiltInCategory.OST_DataDevices,
    BuiltInCategory.OST_CommunicationDevices,
    BuiltInCategory.OST_NurseCallDevices,
    BuiltInCategory.OST_FireAlarmDevices,
    BuiltInCategory.OST_Conduit,
    BuiltInCategory.OST_LightingFixtures,
    BuiltInCategory.OST_StructConnections,
    BuiltInCategory.OST_CableTrayFitting,
    BuiltInCategory.OST_ConduitFitting,
    BuiltInCategory.OST_ElectricalFixtures,
    BuiltInCategory.OST_ElectricalEquipment,
    BuiltInCategory.OST_MechanicalEquipment,
]

# ==== НОВОЕ: Архитектура ====
ARCH_CATS = [
    BuiltInCategory.OST_Walls,
    BuiltInCategory.OST_Floors,
    BuiltInCategory.OST_Ceilings,
    BuiltInCategory.OST_Roofs,
    BuiltInCategory.OST_Doors,
    BuiltInCategory.OST_Windows,
    BuiltInCategory.OST_Stairs,
    BuiltInCategory.OST_StairsRailing,
    BuiltInCategory.OST_Ramps,
    BuiltInCategory.OST_Furniture,
    BuiltInCategory.OST_Casework,
    BuiltInCategory.OST_SpecialityEquipment,
    BuiltInCategory.OST_CurtainWallPanels,
    BuiltInCategory.OST_CurtainWallMullions,
    BuiltInCategory.OST_ShaftOpening,
    ]

# ==== НОВОЕ: Конструктив ====
STRUCT_CATS = [
    BuiltInCategory.OST_StructuralColumns,
    BuiltInCategory.OST_StructuralFraming,
    BuiltInCategory.OST_StructuralFoundation,
    BuiltInCategory.OST_Rebar,
    BuiltInCategory.OST_Coupler,
    BuiltInCategory.OST_Truss,
]

def base_to_internal_delta_z():
    """Коррекция Base Point ↔ Internal Origin (внутренние единицы)."""
    bp = (FilteredElementCollector(doc)
          .OfCategory(BuiltInCategory.OST_ProjectBasePoint)
          .WhereElementIsNotElementType()
          .FirstElement())
    bp_elev = 0.0
    if bp:
        p = bp.get_Parameter(BuiltInParameter.BASEPOINT_ELEVATION_PARAM)
        if p: bp_elev = p.AsDouble()
    io = DB.InternalOrigin.Get(doc)
    io_z = io.SharedPosition.Z if hasattr(io, "SharedPosition") else io.Position.Z
    return bp_elev - io_z

def collect_elements():
    """Собираем элементы из инженерных, архитектурных и конструктивных категорий (без типов)."""
    categories = LINEAR_CATS + NONLINEAR_CATS + ARCH_CATS + STRUCT_CATS
    res, seen = [], set()
    for bic in categories:
        try:
            it = FilteredElementCollector(doc).OfCategory(bic).WhereElementIsNotElementType()
            for el in it:
                eid = el.Id.IntegerValue
                if eid not in seen:
                    seen.add(eid)
                    res.append(el)
        except Exception:
            # На случай, если какая-то категория недоступна в текущей версии/шаблоне
            continue
    return res

def element_height_z(el):
    """Абсолютная (до коррекции) «рабочая» высота элемента:
       - LocationCurve → min(Z) концов (для стояков/стержней);
       - LocationPoint → Point.Z (вставляемые семейства);
       - иначе bbox.Min.Z.
       Возвращает None, если нельзя определить.
    """
    loc = getattr(el, "Location", None)
    if loc:
        crv = getattr(loc, "Curve", None)
        if crv:
            try:
                p0, p1 = crv.GetEndPoint(0), crv.GetEndPoint(1)
                return min(p0.Z, p1.Z)
            except Exception:
                pass
        pt = getattr(loc, "Point", None)
        if pt:
            return pt.Z
    bb = el.get_BoundingBox(None)
    return bb.Min.Z if bb else None

def get_levels_sorted():
    data = [(lvl.Name, lvl.Elevation) for lvl in FilteredElementCollector(doc).OfClass(DB.Level)]
    data.sort(key=lambda x: x[1])
    return data

def closest_lower_level(levels_sorted, z_abs):
    chosen = levels_sorted[0][0]
    for name, elev in levels_sorted:
        if elev <= z_abs: chosen = name
        else: break
    return chosen

def extract_floor_token(level_name):
    """АР_Тест01_+0.000 → 'Тест01' (значение между первой парой '_').
    Если имя уровня не соответствует шаблону, возвращаем None.
    """
    if not level_name:
        return None
    parts = level_name.split('_', 2)
    if len(parts) >= 3 and parts[1].strip():
        return parts[1].strip()
    return None

def set_adsk_floor(el, text_value):
    p = el.LookupParameter(PARAM_NAME)
    if not p:            return False, u"нет параметра"
    if p.IsReadOnly:     return False, u"параметр только для чтения"
    try:
        if p.StorageType == StorageType.String:
            p.Set(u"{}".format(text_value)); return True, u""
        else:
            p.Set(u"{}".format(text_value)); return True, u""
    except Exception as e:
        return False, unicode(e)

# ---------- запуск ----------
elements = collect_elements()
if not elements:
    forms.alert(u"В модели не найдено элементов поддерживаемых категорий.", title=__title__)
    raise SystemExit

levels = get_levels_sorted()
if not levels:
    forms.alert(u"В модели нет уровней.", title=__title__)
    raise SystemExit

delta  = base_to_internal_delta_z()

fail_rows = []
updated = 0

with revit.Transaction(u"ADSK_Этаж: все разделы (ОВВ/ЭОМ/АР/КР)"):
    for el in elements:
        z0 = element_height_z(el)
        if z0 is None:
            fail_rows.append([el.Id.IntegerValue, u"—", u"—", u"нет геометрии/границ"])
            continue

        # Базовая высота элемента (внутренние ед.) с учётом смещения базовой точки:
        z_abs = z0 - delta

        # ==== НОВОЕ: применяем допуск вниз от уровня ====
        # Сдвигаем "критерий выбора уровня" вверх на FLOOR_TOLERANCE_INTERNAL,
        # чтобы элементы чуть ниже уровня попадали на него.
        z_for_level = z_abs + FLOOR_TOLERANCE_INTERNAL
        # ================================================

        lvl_name = closest_lower_level(levels, z_for_level)
        token = extract_floor_token(lvl_name)
        if not token:
            z_m = UnitUtils.ConvertFromInternalUnits(z_abs, UnitTypeId.Meters)
            fail_rows.append([el.Id.IntegerValue, u"{:+.3f}".format(z_m), lvl_name, u"не удалось извлечь номер уровня"])
            continue

        ok, reason = set_adsk_floor(el, token)
        if ok:
            updated += 1
        else:
            z_m = UnitUtils.ConvertFromInternalUnits(z_abs, UnitTypeId.Meters)
            fail_rows.append([el.Id.IntegerValue, u"{:+.3f}".format(z_m), lvl_name, reason or u""])

# Отчёт — только при проблемах
if fail_rows:
    out.set_width(1000)
    out.print_md(u"### Не удалось заполнить 'ADSK_Этаж' у некоторых элементов")
    out.print_table(fail_rows, [u"ID", u"Высота, м", u"Имя уровня", u"Причина"])
else:
    forms.alert(u"Готово. Обновлено элементов: {}".format(updated), title=__title__)