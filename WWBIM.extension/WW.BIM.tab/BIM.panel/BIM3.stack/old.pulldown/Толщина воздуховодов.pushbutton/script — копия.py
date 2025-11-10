# -*- coding: utf-8 -*-
# v1.6 — Толщина стенки из Excel + опционально "Класс герметичности"
# Источники ключей соответствия:
#  A Материал  -> ADSK_Материал обозначение (с нормализацией: оцинк*->оцинковка, черн*/углерод*->сталь черная)
#  B Сечение   -> Маркировка типоразмера (круглый/прямоугольный; фолбэк по геометрии)
#  C Внешняя   -> имя ТИПА внешней изоляции (норм.: *огнезащ*->огнезащита; резерв — параметры "Тип изоляции")
#  D Внутренняя-> имя ТИПА внутренней изоляции (норм.; резерв — параметры "Тип внутренней изоляции")
#  E Размер    -> системный "Размер" (берём max число в мм; фолбэк — шир/выс/диам)
#  F Толщина   -> пишется в ADSK_Толщина стенки
from __future__ import print_function, division
__title__  = u"Толщина\nВоздуховодов"
__author__ = "Влад"
__doc__    = u"""Заполняет параметр ADSK_Толщина стенки у воздуховодов и соед. деталей. Для отражения толщины в Наименовании требуется обновить типы в плагине BIM2B"""
import os, re
try:
    unicode
except NameError:
    unicode = str

import clr
clr.AddReference('RevitAPI'); clr.AddReference('RevitAPIUI')
from Autodesk.Revit.DB import (FilteredElementCollector, BuiltInCategory, BuiltInParameter,
                               Transaction, StorageType, ElementId)
from Autodesk.Revit.UI import (TaskDialog, TaskDialogCommonButtons, TaskDialogResult)

try:
    from pyrevit import forms, script
except Exception:
    forms = None; script = None

uidoc = __revit__.ActiveUIDocument  # noqa
doc   = uidoc.Document

P_ADSK_THICKNESS = u"ADSK_Толщина стенки"
P_CLASS_HERM     = u"Класс герметичности"
P_MAT_DESIGN     = u"ADSK_Материал обозначение"
P_SHAPE_MARK     = u"Маркировка типоразмера"
MM_IN_FT = 304.8

# ---------------- utils: params ----------------
def _get_param(el, name):
    p = el.LookupParameter(name)
    if p: return p
    try:
        et = doc.GetElement(el.GetTypeId())
        return et.LookupParameter(name) if et else None
    except Exception:
        return None

def get_str_param(el, name):
    p = _get_param(el, name)
    if not p: return u""
    try:
        s = p.AsString()
        if s: return unicode(s)
    except Exception: pass
    try:
        s = p.AsValueString()
        if s: return unicode(s)
    except Exception: pass
    return u""

def set_param_value_mm(el, name, value_mm):
    p = el.LookupParameter(name)
    if not p or p.IsReadOnly:
        et = doc.GetElement(el.GetTypeId()); p = et.LookupParameter(name) if et else None
        if not p or p.IsReadOnly: return False
    st = p.StorageType
    if st == StorageType.Double:
        try:
            from Autodesk.Revit.DB import SpecTypeId
            if p.Definition.GetDataType() == SpecTypeId.Length:
                p.Set(float(value_mm)/MM_IN_FT); return True
        except Exception: pass
        try: p.Set(float(value_mm)); return True
        except Exception: return False
    elif st == StorageType.String:
        return p.Set(unicode(value_mm))
    elif st == StorageType.Integer:
        try: p.Set(int(round(float(value_mm)))); return True
        except Exception: return False
    return False

# ---------------- normalize ----------------
def norm_material(s):
    s = unicode(s or u"").strip().lower()
    if s.startswith(u"оцинк") or u"цинк" in s: return u"оцинковка"
    if s.startswith(u"черн") or u"углерод" in s: return u"сталь черная"
    return s or u"-"

def normalize_iso_token(name):
    s = unicode(name or u"").strip().lower()
    if s in (u"", u"-", u"—"): return u"-"
    if u"огнезащ" in s: return u"огнезащита"
    return s

# ---------------- shape / size ----------------
_num_re = re.compile(u"(\\d+[\\.,]?\\d*)")
def get_maxside_from_display_size_mm(el):
    size_str = get_str_param(el, u"Размер")
    nums = []
    for m in _num_re.finditer(size_str):
        try: nums.append(float(m.group(1).replace(",", ".")))
        except Exception: pass
    if nums: return max(nums)
    p_d = el.get_Parameter(BuiltInParameter.RBS_CURVE_DIAMETER_PARAM)
    if p_d and p_d.HasValue and p_d.AsDouble() > 0: return p_d.AsDouble()*MM_IN_FT
    p_w = el.get_Parameter(BuiltInParameter.RBS_CURVE_WIDTH_PARAM)
    p_h = el.get_Parameter(BuiltInParameter.RBS_CURVE_HEIGHT_PARAM)
    if p_w and p_h and p_w.HasValue and p_h.HasValue:
        return max(p_w.AsDouble(), p_h.AsDouble())*MM_IN_FT
    return None

def shape_from_marking(el):
    s = get_str_param(el, P_SHAPE_MARK).strip().lower()
    if u"круг" in s: return u"круглый"
    if u"прям" in s: return u"прямоугольный"
    # fallback по геометрии
    if el.get_Parameter(BuiltInParameter.RBS_CURVE_DIAMETER_PARAM) and \
       el.get_Parameter(BuiltInParameter.RBS_CURVE_DIAMETER_PARAM).AsDouble() > 0: return u"круглый"
    if el.get_Parameter(BuiltInParameter.RBS_CURVE_WIDTH_PARAM) and \
       el.get_Parameter(BuiltInParameter.RBS_CURVE_WIDTH_PARAM).HasValue: return u"прямоугольный"
    return u"-"

# ---------------- insulation tokens ----------------
P_INSUL_EXT_NAMES = (u"Тип изоляции", u"Тип наружной изоляции")
P_INSUL_INT_NAMES = (u"Тип внутренней изоляции", u"Тип внутр. изоляции")

def build_iso_token_map():
    """Возвращает dict: hostId(int) -> (ext_token, int_token). Использует только экземпляры + резерв по параметрам."""
    tokens = {}
    # 1) экземпляры
    try:
        from Autodesk.Revit.DB.Mechanical import DuctInsulation, DuctLining
        for ins in FilteredElementCollector(doc).OfClass(DuctInsulation).WhereElementIsNotElementType():
            try:
                hid = ins.HostElementId.IntegerValue
                tname = unicode(doc.GetElement(ins.GetTypeId()).Name) if ins.GetTypeId() else u""
                ex = normalize_iso_token(tname)
                old = tokens.get(hid, (u"-", u"-"))
                tokens[hid] = (ex, old[1])
            except Exception: pass
        for lin in FilteredElementCollector(doc).OfClass(DuctLining).WhereElementIsNotElementType():
            try:
                hid = lin.HostElementId.IntegerValue
                tname = unicode(doc.GetElement(lin.GetTypeId()).Name) if lin.GetTypeId() else u""
                inn = normalize_iso_token(tname)
                old = tokens.get(hid, (u"-", u"-"))
                tokens[hid] = (old[0], inn)
            except Exception: pass
    except Exception:
        pass
    # 2) резерв: строковые параметры у самих воздуховодов, если чего-то не хватило
    for du in FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_DuctCurves).WhereElementIsNotElementType():
        hid = du.Id.IntegerValue
        ex, inn = tokens.get(hid, (u"-", u"-"))
        if ex == u"-":
            for pn in P_INSUL_EXT_NAMES:
                v = get_str_param(du, pn)
                if v:
                    ex = normalize_iso_token(v); break
        if inn == u"-":
            for pn in P_INSUL_INT_NAMES:
                v = get_str_param(du, pn)
                if v:
                    inn = normalize_iso_token(v); break
        tokens[hid] = (ex, inn)
    return tokens

# ---------------- Excel rules ----------------
HDR_KEYS = {u"материал":"mat", u"сечение":"shape", u"внеш":"iso_ext", u"внутр":"iso_int",
            u"размер":"size_max", u"больш":"size_max", u"включ":"size_max",
            u"толщина стенки":"thk"}

def _norm_hdr(h): return unicode(h or u"").strip().lower().replace("—","-")
def _map_header(headers_row):
    role2idx = {}
    for i,h in enumerate(headers_row):
        key = _norm_hdr(h)
        for probe, role in HDR_KEYS.items():
            if probe in key and role not in role2idx: role2idx[role]=i
    need=("mat","shape","iso_ext","iso_int","size_max","thk")
    miss=[k for k in need if k not in role2idx]
    if miss: raise RuntimeError(u"Не найдены колонки в Excel: " + u", ".join(miss))
    return role2idx
def _to_float(x):
    if x is None: return None
    try: return float(unicode(x).replace(" ","").replace(",", "."))
    except Exception: return None
def _tok(x):
    s = unicode(x or u"").strip()
    if s in (u"", u"-", u"—"): return u"-"
    return s

def read_rules_openpyxl(xlsx_path):
    try: import openpyxl
    except Exception: return None
    wb = openpyxl.load_workbook(xlsx_path, data_only=True, read_only=True)
    ws = wb.active; rows = list(ws.iter_rows(values_only=True)); wb.close()
    if not rows: raise RuntimeError(u"Пустой лист Excel.")
    role = _map_header(rows[0]); rules={}
    for r in rows[1:]:
        if r is None: continue
        mat   = norm_material(_tok(r[role["mat"]]))
        shape = unicode(_tok(r[role["shape"]])).lower()
        if u"круг" in shape: shape=u"круглый"
        elif u"прям" in shape: shape=u"прямоугольный"
        else: continue
        iext  = normalize_iso_token(_tok(r[role["iso_ext"]]))
        iint  = normalize_iso_token(_tok(r[role["iso_int"]]))
        size  = _to_float(r[role["size_max"]]); thk=_to_float(r[role["thk"]])
        if size is None or thk is None: continue
        key=(mat,shape,iext,iint); rules.setdefault(key,[]).append((size,thk))
    for k in list(rules.keys()): rules[k].sort(key=lambda t:t[0])
    return rules

def read_rules_excel_com(xlsx_path):
    clr.AddReference("Microsoft.Office.Interop.Excel")
    import Microsoft.Office.Interop.Excel as Excel
    ex=Excel.ApplicationClass(); ex.Visible=False
    wb=ex.Workbooks.Open(xlsx_path); ws=wb.ActiveSheet; used=ws.UsedRange
    rows=used.Rows.Count; cols=used.Columns.Count
    headers=[used.Cells(1,c).Value2 if used.Cells(1,c) else None for c in range(1,cols+1)]
    role=_map_header(headers); rules={}
    def cell(r,ci): return used.Cells(r,ci+1).Value2 if used.Cells(r,ci+1) else None
    for r in range(2, rows+1):
        mat   = norm_material(_tok(cell(r, role["mat"])))
        shape = unicode(_tok(cell(r, role["shape"]))).lower()
        if u"круг" in shape: shape=u"круглый"
        elif u"прям" in shape: shape=u"прямоугольный"
        else: continue
        iext  = normalize_iso_token(_tok(cell(r, role["iso_ext"])))
        iint  = normalize_iso_token(_tok(cell(r, role["iso_int"])))
        size  = _to_float(cell(r, role["size_max"])); thk=_to_float(cell(r, role["thk"]))
        if size is None or thk is None: continue
        key=(mat,shape,iext,iint); rules.setdefault(key,[]).append((size,thk))
    for k in list(rules.keys()): rules[k].sort(key=lambda t:t[0])
    wb.Close(False); ex.Quit(); return rules

def choose_thickness_mm(rules, mat, shape, iext_tok, iint_tok, size_mm):
    mats = [mat, u"-"]
    isos = [(iext_tok, iint_tok),(iext_tok,u"-"),(u"-",iint_tok),(u"-",u"-")]
    for m in mats:
        for ex, inn in isos:
            steps = rules.get((m,shape,ex,inn))
            if not steps: continue
            for size_max, thk in steps:
                if size_mm <= size_max + 1e-9: return thk
    return None

def parse_hermeticity_from_tokens(ex_tok, in_tok):
    if ex_tok==u"огнезащита" or in_tok==u"огнезащита": return u"Огнезащита"
    return u"-"

def print_fail_table(fails):
    if not script or not fails: return
    out = script.get_output()
    rows = []
    for f in fails:
        rows.append([out.linkify(ElementId(f["id"])), f.get("cat", u""), f.get("reason", u"")])
    out.print_table(rows, columns=[u"ID", u"Категория", u"Причина"],
                    title=u"Элементы без подобранной толщины (кликабельно)")

def get_connected_ducts(fitting):
    ducts=[]
    try:
        mep=getattr(fitting,"MEPModel",None); conns=getattr(mep,"ConnectorManager",None) if mep else None
        if not conns: return ducts
        for c in conns.Connectors:
            if not c.IsConnected: continue
            for r in c.AllRefs:
                o=r.Owner
                if o and o.Category and o.Category.Id.IntegerValue==int(BuiltInCategory.OST_DuctCurves):
                    if all(o.Id!=d.Id for d in ducts): ducts.append(o)
    except Exception: pass
    return ducts

def main():
    # спросить про герметичность
    td=TaskDialog(u"pyRevit"); td.MainInstruction=u"Заполнять «Класс герметичности»?"
    td.MainContent=u"Будет определён по токенам изоляций (например, 'Огнезащита')."
    td.CommonButtons=TaskDialogCommonButtons.Yes|TaskDialogCommonButtons.No
    fill_class=(td.Show()==TaskDialogResult.Yes)

    # Excel
    xlsx=None
    if forms:
        xlsx=forms.pick_file(file_ext='xlsx', init_dir=os.path.dirname(__file__), title=u"Выберите Excel с правилами")
    rules={}
    if xlsx and os.path.exists(xlsx):
        try: rules=read_rules_openpyxl(xlsx) or {}
        except Exception: rules={}
        if not rules:
            try: rules=read_rules_excel_com(xlsx) or {}
            except Exception as e:
                TaskDialog.Show(u"pyRevit", u"Не удалось прочитать Excel.\n{}\nПродолжу {}."
                                .format(e, u"с «Класс герметичности»" if fill_class else u"без действий"))
    # элементы
    sel_ids=list(uidoc.Selection.GetElementIds())
    if sel_ids:
        elems=[doc.GetElement(i) for i in sel_ids]
        ducts=[e for e in elems if e and e.Category and e.Category.Id.IntegerValue==int(BuiltInCategory.OST_DuctCurves)]
        fittings=[e for e in elems if e and e.Category and e.Category.Id.IntegerValue==int(BuiltInCategory.OST_DuctFitting)]
    else:
        ducts=list(FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_DuctCurves).WhereElementIsNotElementType())
        fittings=list(FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_DuctFitting).WhereElementIsNotElementType())

    # токены изоляций по хостам воздуховодов (+резерв)
    iso_tokens_by_host = build_iso_token_map()

    not_matched=[]; cnt_thk_ducts=0; cnt_thk_fits=0; cnt_cls=0
    t=Transaction(doc, u"Толщина стенки / Класс герметичности"); t.Start()

    # воздуховоды
    for du in ducts:
        mat = norm_material(get_str_param(du, P_MAT_DESIGN))
        shape = shape_from_marking(du)
        iext_tok, iint_tok = iso_tokens_by_host.get(du.Id.IntegerValue, (u"-", u"-"))
        size_mm = get_maxside_from_display_size_mm(du)

        thk=None; reason=u""
        if rules and size_mm is not None and shape!=u"-":
            thk = choose_thickness_mm(rules, mat, shape, iext_tok, iint_tok, size_mm)
            if thk is None:
                reason=u"Нет строки Excel: Мат='{}'; Сеч='{}'; Внеш='{}'; Внутр='{}'; Размер={:.1f} мм".format(
                    mat, shape, iext_tok, iint_tok, size_mm)
        else:
            if not rules: reason=u"Не загружены правила Excel"
            elif size_mm is None: reason=u"Не распознан системный «Размер»"
            elif shape==u"-": reason=u"Пустой/неопознанный «Маркировка типоразмера»"

        if thk is None:
            not_matched.append({"id":du.Id.IntegerValue, "cat":unicode(du.Category.Name) if du.Category else u"", "reason":reason})
        else:
            if set_param_value_mm(du, P_ADSK_THICKNESS, thk): cnt_thk_ducts+=1

        if fill_class:
            cls = parse_hermeticity_from_tokens(iext_tok, iint_tok)
            if cls!=u"-" and set_param_value_mm(du, P_CLASS_HERM, cls): cnt_cls+=1

    # фиттинги — берём параметры от самого крупного подключённого воздуховода
    for fi in fittings:
        cons = get_connected_ducts(fi)
        if not cons:
            not_matched.append({"id":fi.Id.IntegerValue,"cat":unicode(fi.Category.Name) if fi.Category else u"",
                                "reason":u"Нет присоединённого воздуховода"})
            continue
        best=None; best_size=-1
        for du in cons:
            s=get_maxside_from_display_size_mm(du)
            if s and s>best_size: best, best_size = du, s
        if not best:
            not_matched.append({"id":fi.Id.IntegerValue,"cat":unicode(fi.Category.Name) if fi.Category else u"",
                                "reason":u"У подключённых воздуховодов «Размер» не распознан"})
            continue

        mat = norm_material(get_str_param(best, P_MAT_DESIGN))
        shape = shape_from_marking(best)
        iext_tok, iint_tok = iso_tokens_by_host.get(best.Id.IntegerValue, (u"-", u"-"))
        size_mm = best_size

        thk=None; reason=u""
        if rules and size_mm is not None and shape!=u"-":
            thk = choose_thickness_mm(rules, mat, shape, iext_tok, iint_tok, size_mm)
            if thk is None:
                reason=u"Нет строки Excel (по воздуховоду): Мат='{}'; Сеч='{}'; Внеш='{}'; Внутр='{}'; Размер={:.1f} мм".format(
                    mat, shape, iext_tok, iint_tok, size_mm)
        else:
            if not rules: reason=u"Не загружены правила Excel"
            elif size_mm is None: reason=u"Не распознан «Размер» у подключённого воздуховода"
            elif shape==u"-": reason=u"У подключённого воздуховода пустой «Маркировка типоразмера»"

        if thk is None:
            not_matched.append({"id":fi.Id.IntegerValue,"cat":unicode(fi.Category.Name) if fi.Category else u"","reason":reason})
        else:
            if set_param_value_mm(fi, P_ADSK_THICKNESS, thk): cnt_thk_fits+=1

        if fill_class:
            cls = parse_hermeticity_from_tokens(iext_tok, iint_tok)
            if cls!=u"-" and set_param_value_mm(fi, P_CLASS_HERM, cls): cnt_cls+=1

    t.Commit()

    TaskDialog.Show(u"pyRevit",
        u"Готово.\nТолщина • Воздуховоды: {}\nТолщина • Соед. детали: {}\n{}"
        .format(cnt_thk_ducts, cnt_thk_fits,
                (u"'{}' обновлено: {}".format(P_CLASS_HERM, cnt_cls) if fill_class else u"'{}' не заполнялся".format(P_CLASS_HERM))))
    print_fail_table(not_matched)

if __name__ == "__main__":
    main()
