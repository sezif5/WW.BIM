# -*- coding: utf-8 -*-
# RoomsToElements_IOS_AR.py
# PyRevit / IronPython (RevitAPI)
from __future__ import print_function

from pyrevit import revit, forms, script
from Autodesk.Revit.DB import *
from System import Math

OUT = script.get_output()

MODE_IOS = u"ИОС — MEP элементы + помещения из AR-ссылки"
MODE_AR  = u"АР — Перекрытия + помещения текущей модели"

OUTSIDE_TXT         = u"Вне помещения"
INCLUDE_HOST_FLOORS = False
STEP_FT             = 1.0
EPS                 = 1e-9
XY_EDGE_TOL         = 1e-3
BBOX_PAD            = 0.5
DEBUG               = False

MEP_CATEGORIES = [
    BuiltInCategory.OST_DuctCurves,
    BuiltInCategory.OST_FlexDuctCurves,
    BuiltInCategory.OST_DuctFitting,
    BuiltInCategory.OST_DuctAccessory,
    BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_CableTray,
    BuiltInCategory.OST_CableTrayFitting,
    BuiltInCategory.OST_Conduit,
    BuiltInCategory.OST_ConduitFitting,
    BuiltInCategory.OST_MechanicalEquipment,
    BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_Sprinklers,
    BuiltInCategory.OST_ElectricalEquipment,
    BuiltInCategory.OST_LightingFixtures,
    BuiltInCategory.OST_ElectricalFixtures,
]

def to_str(val):
    try:
        if val is None:
            return u""
        return unicode(val)
    except:
        try:
            return unicode(str(val), "utf-8")
        except:
            return u""

def room_param_to_string(room, pname):
    if room is None or not pname:
        return u""
    p = room.LookupParameter(pname)
    if not p:
        return u""
    st = p.StorageType
    try:
        if st == StorageType.String:
            s = p.AsString()
            return to_str(s)
        if hasattr(p, "AsValueString"):
            vs = p.AsValueString()
            if vs:
                return to_str(vs)
        if st == StorageType.Double:
            return to_str(p.AsDouble())
        if st == StorageType.Integer:
            return to_str(p.AsInteger())
        if st == StorageType.ElementId:
            eid = p.AsElementId()
            return to_str(eid.IntegerValue) if eid else u""
    except:
        pass
    return u""

def set_elem_param_str(elem, pname, text_value):
    if elem is None or not pname:
        return False
    p = elem.LookupParameter(pname)
    if not p or p.IsReadOnly:
        return False
    try:
        return p.Set(to_str(text_value))
    except:
        return False

def collect_mep_elements(doc):
    elems = []
    for bic in MEP_CATEGORIES:
        try:
            coll = FilteredElementCollector(doc).OfCategory(bic).WhereElementIsNotElementType()
            for e in coll:
                elems.append(e)
        except:
            pass
    return [e for e in elems if hasattr(e, "Id") and e.Document is not None]

def collect_floors(doc):
    try:
        return list(FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_Floors).WhereElementIsNotElementType())
    except:
        return []

def _curve_points(curv):
    try:
        return curv.GetEndPoint(0), curv.GetEndPoint(1)
    except:
        try:
            return curv.Evaluate(0.0, True), curv.Evaluate(1.0, True)
        except:
            return None, None

def _point_seg_dist2(px, py, ax, ay, bx, by):
    vx = bx - ax; vy = by - ay
    wx = px - ax; wy = py - ay
    c1 = vx*wx + vy*wy
    if c1 <= 0.0:
        dx = px - ax; dy = py - ay
        return dx*dx + dy*dy
    c2 = vx*vx + vy*vy
    if c2 <= c1:
        dx = px - bx; dy = py - by
        return dx*dx + dy*dy
    t = c1 / c2
    projx = ax + t*vx; projy = ay + t*vy
    dx = px - projx; dy = py - projy
    return dx*dx + dy*dy

class RoomWrap(object):
    __slots__ = ("room", "loops", "bbmin", "bbmax", "baseZ", "topZ0")
    def __init__(self, link_doc, room):
        self.room = room
        self.loops = []
        opt = SpatialElementBoundaryOptions()
        opt.SpatialElementBoundaryLocation = SpatialElementBoundaryLocation.Finish
        try:
            seglists = room.GetBoundarySegments(opt)
            for seglist in seglists:
                pts = []
                for seg in seglist:
                    crv = seg.GetCurve()
                    a, b = _curve_points(crv)
                    if a is None or b is None:
                        continue
                    if not pts:
                        pts.append(a)
                    pts.append(b)
                if len(pts) >= 3:
                    if abs(pts[0].X - pts[-1].X) > EPS or abs(pts[0].Y - pts[-1].Y) > EPS:
                        pts.append(pts[0])
                    self.loops.append(pts)
        except:
            self.loops = []

        bb = room.get_BoundingBox(None)
        if bb:
            self.bbmin = XYZ(bb.Min.X - BBOX_PAD, bb.Min.Y - BBOX_PAD, -1e9)
            self.bbmax = XYZ(bb.Max.X + BBOX_PAD, bb.Max.Y + BBOX_PAD,  1e9)
        else:
            self.bbmin = XYZ(-1e9, -1e9, -1e9)
            self.bbmax = XYZ( 1e9,  1e9,  1e9)

        self.baseZ, self.topZ0 = room_base_and_top_z(link_doc, room)

    def bbox_contains_xy(self, pt):
        return (self.bbmin.X <= pt.X <= self.bbmax.X and
                self.bbmin.Y <= pt.Y <= self.bbmax.Y)

    def contains_xy_fallback(self, pt):
        tol2 = XY_EDGE_TOL * XY_EDGE_TOL
        x = pt.X; y = pt.Y
        for loop in self.loops:
            for i in range(len(loop) - 1):
                a = loop[i]; b = loop[i+1]
                if _point_seg_dist2(x, y, a.X, a.Y, b.X, b.Y) <= tol2:
                    return True
        inside = False
        for loop in self.loops:
            ln = len(loop)
            for i in range(ln - 1):
                a = loop[i]; b = loop[i+1]
                if (a.Y > y) != (b.Y > y):
                    xinters = (b.X - a.X) * (y - a.Y) / (b.Y - a.Y + 0.0) + a.X
                    if x < xinters:
                        inside = not inside
        return inside

def room_base_and_top_z(link_doc, room):
    baseZ = 0.0
    try:
        lvl = link_doc.GetElement(room.LevelId)
        if lvl: baseZ = lvl.Elevation
    except: pass
    try:
        p_lo = room.get_Parameter(BuiltInParameter.ROOM_LOWER_OFFSET)
        if p_lo: baseZ += p_lo.AsDouble()
    except: pass
    try:
        topZ0 = baseZ + (room.UnboundedHeight or 0.0)
    except:
        topZ0 = baseZ
    return baseZ, topZ0

def collect_faces_for_doc(doc_any):
    top_faces, bottom_faces = [], []
    opt = Options()
    opt.DetailLevel = ViewDetailLevel.Fine
    opt.ComputeReferences = False
    cats = (BuiltInCategory.OST_Floors, BuiltInCategory.OST_StructuralFoundation)
    for bic in cats:
        try:
            elems = FilteredElementCollector(doc_any).OfCategory(bic).WhereElementIsNotElementType().ToElements()
        except:
            elems = []
        for fl in elems:
            try:
                geo = fl.get_Geometry(opt)
                if not geo:
                    continue
                for g in geo:
                    solid = g if isinstance(g, Solid) else None
                    if not solid or solid.Volume <= 1e-9:
                        continue
                    for f in solid.Faces:
                        pf = f if isinstance(f, PlanarFace) else None
                        if not pf: 
                            continue
                        n = pf.FaceNormal
                        if n.Z > EPS:        # верхняя грань
                            top_faces.append(pf)
                        elif n.Z < -EPS:     # нижняя грань
                            bottom_faces.append(pf)
            except:
                continue
    return top_faces, bottom_faces

def plane_z_at_xy(planar_face, x, y):
    n = planar_face.FaceNormal
    if abs(n.Z) < EPS:
        return None
    p0 = planar_face.Origin
    dz = (n.X * (x - p0.X) + n.Y * (y - p0.Y)) / n.Z
    return p0.Z - dz

class FloorSet(object):
    __slots__ = ("name", "to_link", "from_link", "top_faces", "bottom_faces")
    def __init__(self, name, to_link, from_link, top_faces, bottom_faces):
        self.name = name
        self.to_link = to_link      # host -> link
        self.from_link = from_link  # link -> host
        self.top_faces = top_faces              # для нижней границы (ищем ниже)
        self.bottom_faces = bottom_faces        # для верхней границы (ищем выше)

def _face_host_z_at_xy(fs, pf, host_x, host_y):
    try:
        pt_l = fs.to_link.OfPoint(XYZ(host_x, host_y, 0.0))
        xL = pt_l.X; yL = pt_l.Y
        zL = plane_z_at_xy(pf, xL, yL)
        if zL is None:
            return None
        pL = XYZ(xL, yL, zL)
        proj = pf.Project(pL)
        if proj is None:
            return None
        uv = proj.UVPoint
        if not pf.IsInside(uv):
            return None
        pH = fs.from_link.OfPoint(pL)
        return pH.Z
    except:
        return None

def nearest_slab_hostZ_above_hostZ(fsets, host_x, host_y, min_host_z):
    best = None
    for fs in fsets:
        for pf in fs.bottom_faces:
            zH = _face_host_z_at_xy(fs, pf, host_x, host_y)
            if zH is None or zH <= min_host_z + EPS:
                continue
            if (best is None) or (zH < best):
                best = zH
    return best

def nearest_slab_hostZ_below_hostZ(fsets, host_x, host_y, max_host_z):
    best = None
    for fs in fsets:
        for pf in fs.top_faces:
            zH = _face_host_z_at_xy(fs, pf, host_x, host_y)
            if zH is None or zH >= max_host_z - EPS:
                continue
            if (best is None) or (zH > best):
                best = zH
    return best

def xy_inside_room_api(room, baseZ, pt_link_xy, z_hint=None):
    ztest = baseZ + 0.1
    if z_hint is not None:
        if z_hint <= baseZ + 0.1:
            ztest = baseZ + 0.1
        else:
            ztest = min(z_hint, baseZ + 1.0)
    testpt = XYZ(pt_link_xy.X, pt_link_xy.Y, ztest)
    try:
        return bool(room.IsPointInRoom(testpt))
    except:
        return False

def get_sorted_host_levels(doc_host):
    lvls = []
    try:
        for lv in FilteredElementCollector(doc_host).OfClass(Level).ToElements():
            try:
                lvls.append(lv)
            except:
                pass
    except:
        pass
    lvls.sort(key=lambda L: getattr(L, "Elevation", 0.0))
    return lvls

def nearest_upper_level_elev(levels_sorted, z_host):
    for L in levels_sorted:
        try:
            if L.Elevation > z_host + EPS:
                return L.Elevation
        except:
            pass
    return None

def nearest_lower_level_elev(levels_sorted, z_host):
    prev = None
    for L in levels_sorted:
        try:
            if L.Elevation >= z_host - EPS:
                return prev
            prev = L.Elevation
        except:
            pass
    return prev

class RealZRoomTester(object):
    def __init__(self, doc_host, room_link_inst, room_wraps, floor_sets, host_levels_sorted):
        if room_link_inst is None:
            self.room_link_inst = None
            self.room_link_doc  = doc_host
            self.to_room_link   = Transform.Identity
            self.from_room_link = Transform.Identity
        else:
            self.room_link_inst = room_link_inst
            self.room_link_doc  = room_link_inst.GetLinkDocument()
            self.to_room_link   = room_link_inst.GetTotalTransform().Inverse
            self.from_room_link = room_link_inst.GetTotalTransform()

        self.floor_sets  = floor_sets
        self.host_levels = host_levels_sorted
        self.stats = {
            "top_slab_cap": 0, "top_level_cap": 0, "top_no_cap": 0,
            "bot_slab_cap": 0, "bot_level_cap": 0, "bot_no_cap": 0,
        }

        self.roomZ = {}
        for rw in room_wraps:
            r = rw.room
            try:
                key = r.Id.IntegerValue
            except:
                continue
            baseZ, topZ0 = rw.baseZ, rw.topZ0
            try:
                baseH = self.from_room_link.OfPoint(XYZ(0, 0, baseZ)).Z
                topH  = self.from_room_link.OfPoint(XYZ(0, 0, topZ0)).Z
            except:
                baseH, topH = baseZ, topZ0
            self.roomZ[key] = (baseZ, topZ0, baseH, topH)

    def elem_point_in_room(self, rw, pt_host):
        try:
            pt_room = self.to_room_link.OfPoint(pt_host)
        except:
            return False

        baseZ, topZ0, baseH, topH = self.roomZ.get(
            rw.room.Id.IntegerValue,
            (rw.baseZ, rw.topZ0,
             self.from_room_link.OfPoint(XYZ(0,0,rw.baseZ)).Z,
             self.from_room_link.OfPoint(XYZ(0,0,rw.topZ0)).Z)
        )

        if not rw.bbox_contains_xy(pt_room):
            return False
        if not xy_inside_room_api(rw.room, baseZ, pt_room, z_hint=pt_room.Z):
            if not rw.contains_xy_fallback(pt_room):
                return False

        xH, yH, zH = pt_host.X, pt_host.Y, pt_host.Z

        slab_topH = nearest_slab_hostZ_above_hostZ(self.floor_sets, xH, yH, topH)
        if slab_topH is not None:
            real_topH = slab_topH
            self.stats["top_slab_cap"] += 1
        else:
            upper_lvl = nearest_upper_level_elev(self.host_levels, topH)
            if upper_lvl is not None:
                real_topH = upper_lvl
                self.stats["top_level_cap"] += 1
            else:
                real_topH = topH
                self.stats["top_no_cap"] += 1

        slab_botH = nearest_slab_hostZ_below_hostZ(self.floor_sets, xH, yH, baseH)
        if slab_botH is not None:
            real_baseH = slab_botH
            self.stats["bot_slab_cap"] += 1
        else:
            lower_lvl = nearest_lower_level_elev(self.host_levels, baseH)
            if lower_lvl is not None:
                real_baseH = lower_lvl
                self.stats["bot_level_cap"] += 1
            else:
                real_baseH = baseH
                self.stats["bot_no_cap"] += 1

        return (zH >= real_baseH - EPS) and (zH <= real_topH + EPS)

def _dense_points_on_curve(crv):
    pts = []
    try:
        length = crv.ApproximateLength
    except:
        try:
            sp = crv.GetEndPoint(0); ep = crv.GetEndPoint(1)
            dx = ep.X - sp.X; dy = ep.Y - sp.Y; dz = ep.Z - sp.Z
            length = Math.Sqrt(dx*dx + dy*dy + dz*dz)
        except:
            length = 0.0
    if length <= STEP_FT:
        for t in (0.0, 0.25, 0.5, 0.75, 1.0):
            try: pts.append(crv.Evaluate(t, True))
            except: pass
        return pts
    try:
        res = crv.DivideByLength(STEP_FT, False)
        if res:
            for p in res:
                try: pts.append(crv.Evaluate(p, True))
                except: pass
            try: pts.insert(0, crv.Evaluate(0.0, True))
            except: pass
            try: pts.append(crv.Evaluate(1.0, True))
            except: pass
            return pts
    except:
        pass
    steps = int(max(2, min(200, round(length / STEP_FT))))
    for i in range(steps + 1):
        t = float(i) / float(steps)
        try: pts.append(crv.Evaluate(t, True))
        except: pass
    return pts

def floor_sample_points(fl):
    pts = []
    try:
        opt = Options()
        opt.DetailLevel = ViewDetailLevel.Fine
        opt.IncludeNonVisibleObjects = False
        geo = fl.get_Geometry(opt)
    except:
        geo = None
    if not geo:
        return pts

    faces = []
    for g in geo:
        try:
            solid = g if isinstance(g, Solid) else None
            if not solid or solid.Volume <= 1e-9:
                continue
            for f in solid.Faces:
                pf = f if isinstance(f, PlanarFace) else None
                if not pf:
                    continue
                n = pf.FaceNormal
                if n.Z > EPS:
                    faces.append(pf)
        except:
            pass

    uniq, seen = [], set()
    def _push(p):
        if not p: 
            return
        key = (round(p.X, 3), round(p.Y, 3), round(p.Z, 3))
        if key in seen:
            return
        seen.add(key)
        uniq.append(p)

    for pf in faces:
        try:
            mesh = pf.Triangulate()
            try:
                nv = mesh.NumVertices
            except:
                nv = 0
            for i in range(nv):
                try:
                    v = mesh.get_Vertex(i)
                except:
                    try:
                        v = mesh.Vertices[i]
                    except:
                        v = None
                if v: _push(v)
        except:
            pass
        try:
            loops = pf.EdgeLoops
            for loop in loops:
                for edge in loop:
                    try:
                        crv = edge.AsCurve()
                        for p in crv.Tessellate():
                            _push(p)
                    except:
                        pass
        except:
            pass

    MAXPTS = 600
    if len(uniq) > MAXPTS:
        step = max(1, len(uniq) // MAXPTS)
        uniq = uniq[::step]
        if len(uniq) > MAXPTS:
            uniq = uniq[:MAXPTS]

    return uniq

def element_sample_points(elem):
    # Спец-обработка для перекрытий: много опорных точек по поверхности
    try:
        from Autodesk.Revit.DB import Floor as _RvtFloor
        if isinstance(elem, _RvtFloor):
            pts = floor_sample_points(elem)
            if pts:
                return pts
    except:
        pass

    pts = []
    loc = elem.Location
    if loc:
        try:
            p = loc.Point
            if p: pts.append(p)
        except: pass
        try:
            crv = loc.Curve
            if crv:
                pts.extend(_dense_points_on_curve(crv))
        except:
            pass
    try:
        me = elem.MEPModel
        if me and hasattr(me, "ConnectorManager"):
            for c in me.ConnectorManager.Connectors:
                try: pts.append(c.Origin)
                except: pass
    except:
        pass
    try:
        bb = elem.get_BoundingBox(None)
        if bb:
            pts.append(XYZ((bb.Min.X + bb.Max.X) * 0.5,
                           (bb.Min.Y + bb.Max.Y) * 0.5,
                           (bb.Min.Z + bb.Max.Z) * 0.5))
    except:
        pass
    uniq, seen = [], set()
    for p in pts:
        if not p: continue
        key = (round(p.X, 4), round(p.Y, 4), round(p.Z, 4))
        if key in seen: continue
        seen.add(key); uniq.append(p)
    return uniq

def pick_ar_link(doc):
    links = [l for l in FilteredElementCollector(doc).OfClass(RevitLinkInstance).ToElements()
             if l.GetLinkDocument() is not None]
    if not links:
        forms.alert(u"Не найдены загруженные ссылки.", exitscript=True)
    candidates = []
    for li in links:
        try:
            rcount = FilteredElementCollector(li.GetLinkDocument())                .OfCategory(BuiltInCategory.OST_Rooms).GetElementCount()
            if rcount > 0:
                candidates.append(li)
        except:
            pass
    if not candidates:
        forms.alert(u"Среди загруженных ссылок нет документов с помещениями.", exitscript=True)
    preferred = [li for li in candidates if u"АР" in li.Name.upper() or "AR" in li.Name.upper()]
    lst = preferred + [li for li in candidates if li not in preferred]
    if len(lst) == 1:
        return lst[0]
    picked = forms.SelectFromList.show(
        lst,
        u"Выберите ссылку АР (источник помещений)",
        name_attr="Name",
        multiselect=False, button_name=u"Выбрать", width=600, height=400
    )
    if not picked:
        script.exit()
    return picked

def choose_room_param(link_or_host_doc):
    names = set()
    rooms = FilteredElementCollector(link_or_host_doc).OfCategory(BuiltInCategory.OST_Rooms)        .WhereElementIsNotElementType().ToElements()
    for r in rooms:
        try:
            for p in r.Parameters:
                try:
                    d = p.Definition
                    nm = d.Name if d else None
                    if nm: names.add(to_str(nm))
                except:
                    pass
        except:
            pass
    if not names:
        forms.alert(u"Не удалось собрать имена параметров комнат.", exitscript=True)
    names_sorted = sorted(list(names), key=lambda s: s.upper())
    picked = forms.SelectFromList.show(
        names_sorted,
        u"Выберите параметр комнаты (источник)",
        multiselect=False, button_name=u"Использовать", width=600, height=500
    )
    if not picked:
        script.exit()
    return to_str(picked)

def choose_target_param_from_elems(elems, default_name=u"ADSK_Помещение"):
    names = set()
    for el in elems:
        try:
            for p in el.Parameters:
                try:
                    if p and (not p.IsReadOnly) and p.StorageType == StorageType.String:
                        d = p.Definition
                        nm = d.Name if d else None
                        if nm: names.add(to_str(nm))
                except:
                    pass
        except:
            pass
    names.add(to_str(default_name))
    names_sorted = sorted(list(names), key=lambda s: s.upper())
    picked = forms.SelectFromList.show(
        names_sorted,
        u"Выберите параметр элемента (приёмник)",
        multiselect=False, button_name=u"Использовать", width=600, height=500
    )
    if not picked:
        script.exit()
    return to_str(picked)

def build_floor_sets_selected(doc_host, selected_link_inst):
    fsets = []
    if INCLUDE_HOST_FLOORS:
        top_h, bot_h = collect_faces_for_doc(doc_host)
        if top_h or bot_h:
            ident = Transform.Identity
            fsets.append(FloorSet(u"[HOST]", ident.Inverse, ident, top_h, bot_h))
    ldoc = selected_link_inst.GetLinkDocument()
    top_l, bot_l = collect_faces_for_doc(ldoc)
    if top_l or bot_l:
        T = selected_link_inst.GetTotalTransform()
        fsets.append(FloorSet(selected_link_inst.Name, T.Inverse, T, top_l, bot_l))
    return fsets

def build_floor_sets_host_only(doc_host):
    fsets = []
    top_h, bot_h = collect_faces_for_doc(doc_host)
    if top_h or bot_h:
        ident = Transform.Identity
        fsets.append(FloorSet(u"[HOST]", ident.Inverse, ident, top_h, bot_h))
    return fsets

def main():
    doc = revit.doc

    mode = forms.SelectFromList.show(
        [MODE_IOS, MODE_AR],
        title=u"Выберите режим работы",
        multiselect=False,
        button_name=u"Далее",
        width=520, height=180
    )
    if not mode:
        script.exit()

    total = 0
    set_ok = 0
    set_out = 0
    set_fail = 0

    if mode == MODE_IOS:
        ar_link = pick_ar_link(doc)
        link_doc = ar_link.GetLinkDocument()
        src_room_param = choose_room_param(link_doc)
        elems = collect_mep_elements(doc)
        if not elems:
            forms.alert(u"Инженерные элементы не найдены в активной модели.", exitscript=True)
        tgt_elem_param = choose_target_param_from_elems(elems, u"ADSK_Помещение")

        rooms = FilteredElementCollector(link_doc).OfCategory(BuiltInCategory.OST_Rooms)            .WhereElementIsNotElementType().ToElements()
        wraps = [RoomWrap(link_doc, r) for r in rooms]
        if not wraps:
            forms.alert(u"Помещения в выбранной ссылке не найдены.", exitscript=True)

        floor_sets = build_floor_sets_selected(doc, ar_link)
        host_levels = get_sorted_host_levels(doc)
        tester = RealZRoomTester(doc, ar_link, wraps, floor_sets, host_levels)
        txname = u"ИОС: Помещение → MEP (AR link + Floors/Levels)"
    else:
        link_doc = doc
        src_room_param = choose_room_param(doc)
        elems = collect_floors(doc)
        if not elems:
            forms.alert(u"Перекрытия не найдены в активной модели.", exitscript=True)
        tgt_elem_param = choose_target_param_from_elems(elems, u"ADSK_Помещение")

        rooms = FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_Rooms)            .WhereElementIsNotElementType().ToElements()
        wraps = [RoomWrap(doc, r) for r in rooms]
        if not wraps:
            forms.alert(u"Помещения в активной модели не найдены.", exitscript=True)

        floor_sets = build_floor_sets_host_only(doc)
        host_levels = get_sorted_host_levels(doc)
        tester = RealZRoomTester(doc, None, wraps, floor_sets, host_levels)
        txname = u"АР: Помещение → Перекрытия (Host Rooms + Floors/Levels)"

    t = Transaction(doc, txname)
    t.Start()
    try:
        for el in elems:
            total += 1
            pts = element_sample_points(el)
            if not pts:
                if set_elem_param_str(el, tgt_elem_param, OUTSIDE_TXT):
                    set_out += 1
                else:
                    set_fail += 1
                continue

            room_ids = set()
            rooms_found = []
            for pth in pts:
                for rw in wraps:
                    if tester.elem_point_in_room(rw, pth):
                        rid = rw.room.Id.IntegerValue
                        if rid not in room_ids:
                            room_ids.add(rid)
                            rooms_found.append(rw.room)

            if not rooms_found:
                if DEBUG:
                    OUT.print_md(u"- Элемент {}: вне помещения ({} точек)".format(el.Id.IntegerValue, len(pts)))
                if set_elem_param_str(el, tgt_elem_param, OUTSIDE_TXT):
                    set_out += 1
                else:
                    set_fail += 1
                continue

            vals, seen_vals = [], set()
            for r in rooms_found:
                v = room_param_to_string(r, src_room_param).strip()
                if v and v not in seen_vals:
                    seen_vals.add(v)
                    vals.append(v)
            joined = u", ".join(vals) if vals else OUTSIDE_TXT

            if set_elem_param_str(el, tgt_elem_param, joined):
                set_ok += 1
            else:
                set_fail += 1
    finally:
        t.Commit()

    OUT.print_md(u"### Готово")
    OUT.print_md(u"*Режим:* **{0}**".format(mode))
    OUT.print_md(u"*Всего элементов:* **{0}**".format(total))
    OUT.print_md(u"*Заполнено значением комнаты:* **{0}**".format(set_ok))
    OUT.print_md(u"*Поставлено \"{0}\":* **{1}**".format(OUTSIDE_TXT, set_out))
    if set_fail:
        OUT.print_md(u"*Не удалось записать параметр (read-only/нет параметра):* **{0}**".format(set_fail))

if __name__ == "__main__":
    main()
