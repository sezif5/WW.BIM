# -*- coding: utf-8 -*-
from __future__ import print_function, division

try:
    from pyrevit import revit, DB, forms
except Exception as e:
    raise Exception("Нужно запускать из pyRevit. {0}".format(e))

doc = revit.doc
uidoc = revit.uidoc

import math

def _to_mm(feet_value):
    try:
        return DB.UnitUtils.ConvertFromInternalUnits(feet_value, DB.UnitTypeId.Millimeters)
    except:
        return DB.UnitUtils.ConvertFromInternalUnits(feet_value, DB.DisplayUnitType.DUT_MILLIMETERS)

def _mm_to_internal(mm_value):
    try:
        return DB.UnitUtils.ConvertToInternalUnits(mm_value, DB.UnitTypeId.Millimeters)
    except:
        return DB.UnitUtils.ConvertToInternalUnits(mm_value, DB.DisplayUnitType.DUT_MILLIMETERS)

def _round_mm(v):
    return int(math.floor(v + 0.5))

def get_all_levels_sorted(doc):
    lvls = list(DB.FilteredElementCollector(doc).OfClass(DB.Level).ToElements())
    lvls.sort(key=lambda l: l.Elevation)
    return lvls

def find_upper(levels_sorted, top_z):
    for lvl in levels_sorted:
        if lvl.Elevation >= top_z:
            return lvl
    return None

def find_lower(levels_sorted, bot_z):
    for lvl in reversed(levels_sorted):
        if lvl.Elevation <= bot_z:
            return lvl
    return None

def set_mm(elem, pname, mm):
    p = elem.LookupParameter(pname)
    if not p or p.IsReadOnly:
        return False
    st = p.StorageType
    if st == DB.StorageType.Double:
        p.Set(_mm_to_internal(float(mm)))
        return True
    if st == DB.StorageType.Integer:
        p.Set(int(_round_mm(mm)))
        return True
    if st == DB.StorageType.String:
        p.Set(str(int(_round_mm(mm))))
        return True
    return False

# -------- SOLID/MESH extents (robust) --------
def _geo_options():
    opt = DB.Options()
    try:
        opt.DetailLevel = DB.ViewDetailLevel.Fine
    except: pass
    opt.ComputeReferences = True
    opt.IncludeNonVisibleObjects = True
    try:
        opt.View = doc.ActiveView
    except: pass
    return opt

def _acc_pt(pt, tr, acc):
    try:
        p = tr.OfPoint(pt) if tr else pt
    except: p = pt
    z = p.Z
    if acc[0] is None or z < acc[0]:
        acc[0] = z
    if acc[1] is None or z > acc[1]:
        acc[1] = z

def _consume_mesh(mesh, tr, acc):
    if not mesh: return
    try:
        count = mesh.NumVertices
        for i in range(count):
            _acc_pt(mesh.get_Vertex(i), tr, acc)
    except:
        try:
            for v in mesh.Vertices:
                _acc_pt(v, tr, acc)
        except: pass

def _consume_edges(edges, tr, acc):
    if not edges: return
    eit = edges.GetEnumerator()
    while eit.MoveNext():
        e = eit.Current
        try:
            pts = e.Tessellate()
        except:
            pts = []
        for p in pts:
            _acc_pt(p, tr, acc)

def _consume_solid(solid, tr, acc):
    # Не отбрасываем solids с нулевым Volume — у них всё равно есть грани/рёбра
    try:
        faces = solid.Faces
        if faces:
            it = faces.GetEnumerator()
            while it.MoveNext():
                f = it.Current
                try:
                    m = f.Triangulate()
                    _consume_mesh(m, tr, acc)
                except:
                    pass
        # подстраховка: пройдёмся по рёбрам
        try:
            _consume_edges(solid.Edges, tr, acc)
        except: pass
    except: pass

def _walk(go, tr, acc):
    # порядок проверок важен
    if isinstance(go, DB.Solid):
        _consume_solid(go, tr, acc); return
    if isinstance(go, DB.Mesh):
        _consume_mesh(go, tr, acc); return
    if isinstance(go, DB.GeometryInstance):
        new_t = tr
        try:
            t = go.Transform
            new_t = (new_t.Multiply(t) if new_t else t)
        except: pass
        # сначала символ-геометрия (стабильнее), затем инстанс
        sub = None
        try:
            sub = go.GetSymbolGeometry()
        except: pass
        if sub is None:
            try:
                sub = go.GetInstanceGeometry()
            except: sub = None
        if sub:
            it = sub.GetEnumerator()
            while it.MoveNext():
                _walk(it.Current, new_t, acc)
        return
    if isinstance(go, DB.GeometryElement):
        it = go.GetEnumerator()
        while it.MoveNext():
            _walk(it.Current, tr, acc)
        return
    # последнее средство: попробуем взять свой bbox
    try:
        bb = go.get_BoundingBox()
        if bb:
            _acc_pt(bb.Min, tr, acc)
            _acc_pt(bb.Max, tr, acc)
    except: pass

def get_top_bottom_solid(elem):
    try:
        geo = elem.get_Geometry(_geo_options())
        if not geo:
            return None, None
        acc = [None, None]  # minZ, maxZ
        _walk(geo, DB.Transform.Identity, acc)
        if acc[0] is None or acc[1] is None:
            return None, None
        return acc[1], acc[0]  # top, bottom
    except:
        return None, None

def process(elements, levels_sorted, mode, up=None, low=None):
    P_TOP = u"Видимость_верх"
    P_BOTTOM = u"Видимость_низ"
    stats = dict(processed=0, skipped_geom=0, skipped_levels=0, skipped_params=0)

    t = DB.Transaction(doc, u"Видимость верх/низ (SOLID v2)")
    t.Start()
    try:
        for el in elements:
            cat = el.Category
            if not cat or cat.Id.IntegerValue != int(DB.BuiltInCategory.OST_GenericModel):
                continue
            top, bot = get_top_bottom_solid(el)
            if top is None or bot is None:
                stats["skipped_geom"] += 1
                continue

            if mode == "auto":
                u_lvl = find_upper(levels_sorted, top)
                l_lvl = find_lower(levels_sorted, bot)
            else:
                u_lvl = up or find_upper(levels_sorted, top)
                l_lvl = low or find_lower(levels_sorted, bot)

            if u_lvl is None and l_lvl is None:
                stats["skipped_levels"] += 1
                continue

            d_top_ft = (u_lvl.Elevation - top) if u_lvl is not None else None
            d_bot_ft = (bot - l_lvl.Elevation) if l_lvl is not None else None
            if d_top_ft is not None and d_top_ft < 0: d_top_ft = 0.0
            if d_bot_ft is not None and d_bot_ft < 0: d_bot_ft = 0.0

            ok = False
            if d_top_ft is not None:
                ok |= set_mm(el, P_TOP, _to_mm(d_top_ft))
            if d_bot_ft is not None:
                ok |= set_mm(el, P_BOTTOM, _to_mm(d_bot_ft))
            if ok: stats["processed"] += 1
            else:  stats["skipped_params"] += 1

        t.Commit()
    except:
        t.RollBack()
        raise
    return stats

def _pick_levels(levels_sorted):
    if not levels_sorted:
        forms.alert(u"В модели нет уровней.", warn_icon=True)
        return None, None
    up = forms.SelectFromList.show(levels_sorted, title=u"Выберите ВЕРХНИЙ уровень (Esc — пропустить)", name_attr='Name')
    low = forms.SelectFromList.show(levels_sorted, title=u"Выберите НИЖНИЙ уровень (Esc — пропустить)", name_attr='Name')
    return up, low

def main():
    levels_sorted = get_all_levels_sorted(doc)
    if not levels_sorted:
        forms.alert(u"В документе не найдены уровни. Скрипт рассчитан на проектный файл, а не на редактор семейств.", title=u"Нет уровней", warn_icon=True)
        return

    choice = forms.CommandSwitchWindow.show(
        [u"Авто: все обобщенные модели", u"Выбор: по выделению + ручные уровни"],
        title=u"Видимость_верх/низ (SOLID v2)"
    )
    if not choice: return

    if choice.startswith(u"Авто"):
        elems = list(DB.FilteredElementCollector(doc).OfCategory(DB.BuiltInCategory.OST_GenericModel).WhereElementIsNotElementType().ToElements())
        stats = process(elems, levels_sorted, "auto")
    else:
        ids = list(uidoc.Selection.GetElementIds())
        if not ids:
            forms.alert(u"Ничего не выбрано.", warn_icon=True); return
        elems = [doc.GetElement(i) for i in ids]
        elems = [e for e in elems if e.Category and e.Category.Id.IntegerValue == int(DB.BuiltInCategory.OST_GenericModel)]
        if not elems:
            forms.alert(u"В выделении нет обобщенных моделей.", warn_icon=True); return
        up, low = _pick_levels(levels_sorted)
        stats = process(elems, levels_sorted, "manual", up, low)

    forms.alert(u"Готово.\nИзменено: {p}\nНет solid/mesh: {g}\nНет уровней: {l}\nПараметры недоступны: {pp}".format(
        p=stats["processed"], g=stats["skipped_geom"], l=stats["skipped_levels"], pp=stats["skipped_params"]
    ))

if __name__ == "__main__":
    main()
