# -*- coding: utf-8 -*-
__title__  = u"Номер секции\nпо объёму"
__author__ = u"Vlad"
__doc__    = u"""Выберите объёмы категории обобщенная модель.
Скрипт найдёт все элементы (инженерные + общестроительные), пересекающиеся с объёмом,
и запишет им ADSK_Номер секции. Подкомпоненты семейств тоже заполняются.
Отчёт печатается только по проблемам (кликабельные ID)."""

from pyrevit import revit, script, forms
import Autodesk.Revit.DB as DB
from System.Collections.Generic import List
from Autodesk.Revit.DB import (
    BuiltInCategory, ElementId, FilteredElementCollector,
    ElementIntersectsSolidFilter, ElementMulticategoryFilter,
    BooleanOperationsUtils, BooleanOperationsType, SolidUtils,
    FamilyInstance, BuiltInParameter, StorageType, RevitLinkInstance
)
from Autodesk.Revit.UI.Selection import ObjectType, ISelectionFilter

# ---------- env ----------
doc   = revit.doc
uidoc = revit.uidoc
out   = script.get_output()
out.close_others(all_open_outputs=True)

SECTION_PARAM = u"ADSK_Номер секции"
VOL_CAT       = BuiltInCategory.OST_GenericModel

# ====== целевые категории: ОГС + МЕП ======
TARGET_CATS = [
    # ОГС
    BuiltInCategory.OST_Walls,
    BuiltInCategory.OST_Floors,
    BuiltInCategory.OST_Roofs,
    BuiltInCategory.OST_Ceilings,
    BuiltInCategory.OST_Columns,
    BuiltInCategory.OST_StructuralColumns,
    BuiltInCategory.OST_StructuralFraming,
    BuiltInCategory.OST_StructuralFoundation,
    BuiltInCategory.OST_StructuralStiffener,
    BuiltInCategory.OST_Stairs,
    BuiltInCategory.OST_Railings,
    BuiltInCategory.OST_CurtainWallPanels,
    BuiltInCategory.OST_CurtainWallMullions,
    BuiltInCategory.OST_Doors,
    BuiltInCategory.OST_Windows,
    BuiltInCategory.OST_GenericModel,

    # MEP
    BuiltInCategory.OST_DuctCurves,
    BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_FlexDuctCurves,
    BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_DuctFitting,
    BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_DuctAccessory,
    BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_DuctTerminal,
    BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_Sprinklers,
    BuiltInCategory.OST_MechanicalEquipment,
    BuiltInCategory.OST_CableTray,
    BuiltInCategory.OST_CableTrayFitting,
    BuiltInCategory.OST_Conduit,
    BuiltInCategory.OST_ConduitFitting,
    BuiltInCategory.OST_ElectricalEquipment,
    BuiltInCategory.OST_ElectricalFixtures,
    BuiltInCategory.OST_LightingDevices,
    BuiltInCategory.OST_LightingFixtures,
    BuiltInCategory.OST_DataDevices,
    BuiltInCategory.OST_SpecialityEquipment,
]

# ---------- helpers ----------
def family_label(el):
    try:
        et = doc.GetElement(el.GetTypeId())
        if et:
            fam = getattr(et, "FamilyName", None)
            typ = getattr(et, "Name", None)
            if fam and typ: return u"%s : %s" % (fam, typ)
            if fam: return fam
            if typ: return typ
    except: pass
    try:
        if hasattr(el, "Symbol") and el.Symbol:
            return u"%s : %s" % (el.Symbol.Family.Name, el.Symbol.Name)
    except: pass
    return (el.Category.Name if el.Category else u"")

def _as_str(p):
    if not p: return None
    try:
        s = p.AsString()
        if s: return s.strip()
    except: pass
    try:
        s = p.AsValueString()
        if s: return s.strip()
    except: pass
    return None

def get_string_param(el, name):
    p = el.LookupParameter(name)
    return _as_str(p) if p else None

def set_string_param(el, name, value):
    p = el.LookupParameter(name)
    if not p:
        return False, u"нет параметра «%s»" % name
    if p.IsReadOnly:
        return False, u"параметр только для чтения"
    try:
        p.Set(u"%s" % value)
        return True, u""
    except Exception as e:
        try:  return False, unicode(e)
        except: return False, str(e)

def solids_of_element(el):
    """Вернёт единый Solid элемента (объединение), либо пустой список."""
    try:
        opt = DB.Options()
        opt.DetailLevel = DB.ViewDetailLevel.Fine
        opt.IncludeNonVisibleObjects = True
        geo = el.get_Geometry(opt)
        if not geo: return []
        def _acc(giter, cur):
            for g in giter:
                if isinstance(g, DB.Solid) and g.Volume > 1e-9:
                    cur = g if cur is None else BooleanOperationsUtils.ExecuteBooleanOperation(
                        cur, g, BooleanOperationsType.Union)
                elif isinstance(g, DB.GeometryInstance):
                    cur = _acc(g.GetInstanceGeometry(), cur)
            return cur
        union = _acc(geo, None)
        return [union] if union else []
    except:
        return []

def multicategory_filter():
    ids = List[ElementId]()             # корректный .NET List<ElementId>
    for bic in TARGET_CATS:
        ids.Add(ElementId(int(bic)))
    return ElementMulticategoryFilter(ids)

def iter_with_subcomponents(root):
    """Сам элемент + все вложенные FamilyInstance подкомпоненты (без дублей)."""
    stack = [root]
    visited = set([root.Id.IntegerValue])
    while stack:
        el = stack.pop()
        yield el
        if isinstance(el, FamilyInstance):
            try:
                for sid in (el.GetSubComponentIds() or []):
                    if sid.IntegerValue in visited: continue
                    sub = doc.GetElement(sid)
                    if sub:
                        visited.add(sid.IntegerValue)
                        stack.append(sub)
            except: pass

# ---------- выбор объёмов (один PickObjects) ----------
class HostGenericModelFilter(ISelectionFilter):
    def AllowElement(self, e):
        try:
            return e.Category and e.Category.Id == ElementId(int(VOL_CAT))
        except:
            return False
    def AllowReference(self, reference, position): return True

class AllowAnyLinked(ISelectionFilter):
    def AllowElement(self, e): return True
    def AllowReference(self, reference, position): return True

def pick_volumes():
    opts = [u"В связях (рекомендуется)", u"В текущем файле"]
    try:
        choice = forms.CommandSwitchWindow.show(opts, message=u"Где находятся объёмы секций?")
    except:
        choice = forms.SelectFromList.show(opts, title=u"Где находятся объёмы секций?",
                                           multiselect=False, button_name=u"Выбрать")
    if not choice:
        script.exit()

    in_links = (choice == opts[0])
    msg = (u"Выберите объёмы секций (Generic Model) в СВЯЗЯХ.\n"
           u"Можно выбрать несколько; завершите зелёной галочкой.") if in_links \
           else (u"Выберите объёмы секций (Generic Model) в ТЕКУЩЕМ файле.\n"
                 u"Можно выбрать несколько; завершите зелёной галочкой.")
    forms.alert(msg, title=__title__, warn_icon=False)

    try:
        if in_links:
            refs = uidoc.Selection.PickObjects(ObjectType.LinkedElement, AllowAnyLinked())
        else:
            refs = uidoc.Selection.PickObjects(ObjectType.Element, HostGenericModelFilter())
    except Exception:
        refs = []

    if not refs:
        forms.alert(u"Ничего не выбрано.", title=__title__)
        script.exit()

    return in_links, refs

# ---------- MAIN ----------
in_links, refs = pick_volumes()

volumes = []   # [{solid, section, label}]
skipped = []   # сообщения по пропущенным объёмам

for r in refs:
    try:
        if in_links:
            # выбран элемент в связи
            linkinst = doc.GetElement(r.ElementId)
            if not isinstance(linkinst, RevitLinkInstance):
                continue
            ldoc  = linkinst.GetLinkDocument()
            lelem = ldoc.GetElement(r.LinkedElementId)
            if not lelem or not lelem.Category:
                continue
            # проверка категории
            if lelem.Category.Id.IntegerValue != int(VOL_CAT) and \
               (lelem.Category.Name not in [u"Обобщенные модели", u"Generic Models"]):
                continue
            sols = solids_of_element(lelem)
            if not sols:
                skipped.append(u"%s (нет геометрии)" % family_label(lelem)); continue
            host_solid = SolidUtils.CreateTransformed(sols[0], linkinst.GetTotalTransform())
            sec = get_string_param(lelem, SECTION_PARAM)
            if not sec:
                sec = forms.ask_for_string(default=u"", prompt=u"Введите номер секции для объёма:\n%s" % family_label(lelem),
                                           title=__title__)
                if not sec:
                    skipped.append(u"%s (номер секции не задан)" % family_label(lelem)); continue
            volumes.append({"solid": host_solid, "section": sec, "label": family_label(lelem)})
        else:
            # выбран элемент в хосте
            helem = doc.GetElement(r.ElementId)
            if not helem or not helem.Category:
                continue
            if helem.Category.Id.IntegerValue != int(VOL_CAT) and \
               (helem.Category.Name not in [u"Обобщенные модели", u"Generic Models"]):
                continue
            sols = solids_of_element(helem)
            if not sols:
                skipped.append(u"%s (нет геометрии)" % family_label(helem)); continue
            host_solid = sols[0]
            sec = get_string_param(helem, SECTION_PARAM)
            if not sec:
                sec = forms.ask_for_string(default=u"", prompt=u"Введите номер секции для объёма:\n%s" % family_label(helem),
                                           title=__title__)
                if not sec:
                    skipped.append(u"%s (номер секции не задан)" % family_label(helem)); continue
            volumes.append({"solid": host_solid, "section": sec, "label": family_label(helem)})
    except Exception as e:
        msg = getattr(e, "Message", str(e))
        skipped.append(u"Ошибка чтения выбранного объёма: %s" % msg)

if not volumes:
    forms.alert(u"Нет корректных объёмов для обработки.", title=__title__)
    script.exit()

# подготавливаем фильтры
mcat_filter = multicategory_filter()
assigned = {}   # eid -> section (для отслеживания конфликтов)
fails    = []   # проблемы записи в элементы
conflict = []   # элемент попал в разные объёмы (разные секции)

with revit.Transaction(u"Заполнение «%s» по объёмам" % SECTION_PARAM):
    for v in volumes:
        solid = v["solid"]
        sec   = v["section"]

        # набор кандидатов: нужные категории + пересечение с solid
        col = (FilteredElementCollector(doc)
               .WhereElementIsNotElementType()
               .WherePasses(mcat_filter)
               .WherePasses(ElementIntersectsSolidFilter(solid)))

        for el in col:
            # пропустим экземпляры связей на всякий случай
            if isinstance(el, RevitLinkInstance):
                continue

            eid = el.Id.IntegerValue
            # если уже присвоена другая секция — фиксируем конфликт, не перетираем
            if eid in assigned and assigned[eid] != sec:
                conflict.append([out.linkify(el.Id), family_label(el), u"%s → %s" % (assigned[eid], sec)])
                continue

            ok_any = True
            for tgt in iter_with_subcomponents(el):
                ok, reason = set_string_param(tgt, SECTION_PARAM, sec)
                if not ok:
                    ok_any = False
                    fails.append([out.linkify(tgt.Id), family_label(tgt), (reason or u"")])

            if ok_any:
                assigned[eid] = sec

# ----- отчёты (только по проблемам) -----
if skipped:
    out.print_md(u"### Пропущенные объёмы")
    for msg in skipped:
        out.print_md(u"- %s" % msg)

if conflict:
    out.set_width(1100)
    out.print_md(u"### Конфликты (элемент попал в разные объёмы)")
    out.print_table(conflict, [u"ID", u"Семейство/Тип", u"Секции (старое → новое)"])

if fails:
    out.set_width(1100)
    out.print_md(u"### Не удалось записать параметр у некоторых элементов")
    out.print_table(fails, [u"ID", u"Семейство/Тип", u"Причина"])
