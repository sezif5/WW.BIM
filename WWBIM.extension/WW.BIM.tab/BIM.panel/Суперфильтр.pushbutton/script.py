# -*- coding: utf-8 -*-
# SuperFilter (3D/планы/разрезы) для pyRevit (IronPython)
# Версия: WinForms v2.7
from __future__ import print_function, division

import clr
from collections import OrderedDict, defaultdict

from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId, StorageType, View,
    Category, CategoryType, BuiltInCategory, BuiltInParameter, ParameterElement
)
from Autodesk.Revit.Exceptions import InvalidOperationException
from pyrevit import revit, script

doc = revit.doc
uidoc = revit.uidoc
active_view = doc.ActiveView

# ---------------- Helpers ----------------
OST_CAMERAS_INT = int(BuiltInCategory.OST_Cameras)
_PROJECT_DEF_NAMES = None  # кэш имён определений параметров проекта

def ensure_view_supported(view):
    if view is None or view.IsTemplate:
        from pyrevit import forms
        forms.alert(u'Активируйте обычный вид (не шаблон) и запустите снова.', title=u'Суперфильтр')
        raise SystemExit

def _to_unicode(s):
    try:
        return unicode(s)
    except:
        try:
            return s.decode('utf-8')
        except:
            return s

def _is_camera(el):
    try:
        cat = el.Category
        if cat and cat.Id and cat.Id.IntegerValue == OST_CAMERAS_INT:
            return True
    except:
        pass
    return False

def _eid_to_disp(eid):
    if not isinstance(eid, ElementId):
        return _to_unicode(eid)
    try:
        cat = Category.GetCategory(doc, eid)
        if cat is not None:
            return _to_unicode(cat.Name)
    except:
        pass
    try:
        refel = doc.GetElement(eid)
        if refel:
            nm = None
            try:
                nm = refel.Name
            except:
                nm = None
            if nm:
                return u'{0} [{1}]'.format(_to_unicode(nm), eid.IntegerValue)
    except:
        pass
    return u'Id:{0}'.format(eid.IntegerValue)

def collect_candidates(only_visible=True):
    if only_visible:
        col = FilteredElementCollector(doc, active_view.Id)
    else:
        col = FilteredElementCollector(doc)
    col = col.WhereElementIsNotElementType()
    result = []
    for el in col:
        if _is_camera(el):
            continue
        cat = el.Category
        if cat and cat.CategoryType == CategoryType.Model:
            result.append(el)
    return result

def _is_builtin_param(p):
    try:
        idef = p.Definition
        bip = getattr(idef, 'BuiltInParameter', None)
        if bip is None:
            return False
        try:
            return bip != BuiltInParameter.INVALID
        except:
            try:
                return int(bip) != -1
            except:
                return False
    except:
        return False

def _is_shared_param(p):
    try:
        return bool(getattr(p, 'IsShared', False))
    except:
        return False

def _ensure_project_def_names():
    global _PROJECT_DEF_NAMES
    if _PROJECT_DEF_NAMES is not None:
        return
    names = set()
    try:
        for pe in FilteredElementCollector(doc).OfClass(ParameterElement):
            try:
                d = pe.GetDefinition()
                nm = d.Name
                if nm:
                    names.add(nm)
            except:
                pass
    except:
        pass
    _PROJECT_DEF_NAMES = names

def _is_project_param_def(defn):
    # 1) Пытаемся спросить у BindingMap напрямую
    try:
        if doc.ParameterBindings.Contains(defn):
            return True
    except:
        pass
    # 2) Fallback по кэшу имён ParameterElement'ов
    _ensure_project_def_names()
    try:
        return defn.Name in _PROJECT_DEF_NAMES
    except:
        return False

def _classify_param(p):
    # Возвращает 'builtin' | 'shared' | 'project' | 'family'
    if _is_builtin_param(p):
        return 'builtin'
    if _is_shared_param(p):
        return 'shared'
    try:
        defn = p.Definition
    except:
        defn = None
    if defn is not None and _is_project_param_def(defn):
        return 'project'
    return 'family'

def _include_param(p, include_family):
    kind = _classify_param(p)
    if kind == 'family' and not include_family:
        return False
    return True

def build_param_index(only_visible=True, include_type=True, include_family=False, sample_per_cat=30):
    elems = collect_candidates(only_visible)
    by_cat = defaultdict(list)
    for el in elems:
        try:
            cid = el.Category.Id.IntegerValue
        except:
            continue
        if len(by_cat[cid]) < sample_per_cat:
            by_cat[cid].append(el)

    param_index = {}
    type_cache = {}
    out = script.get_output()
    try:
        pb = out.create_progress_bar(len(by_cat), title=u'Подготовка параметров...')
    except:
        pb = None

    try:
        for cid, samples in by_cat.items():
            if pb: pb.update()
            for el in samples:
                for p in el.Parameters:
                    if not _include_param(p, include_family):
                        continue
                    defn = p.Definition
                    if not defn:
                        continue
                    pname = defn.Name
                    if pname not in param_index:
                        param_index[pname] = p.StorageType
                if include_type:
                    tid = el.GetTypeId()
                    if tid and tid.IntegerValue not in type_cache:
                        typ = doc.GetElement(tid)
                        type_cache[tid.IntegerValue] = True
                        if typ:
                            for p in typ.Parameters:
                                if not _include_param(p, include_family):
                                    continue
                                defn = p.Definition
                                if not defn:
                                    continue
                                pname = defn.Name
                                if pname not in param_index:
                                    param_index[pname] = p.StorageType
    finally:
        if pb: pb.close()

    names = sorted(param_index.keys(), key=lambda s: s.lower())
    return names, param_index

def collect_values_for_param(pname, storage, only_visible=True, include_type=True, include_family=False):
    elems = collect_candidates(only_visible)
    values = OrderedDict()
    has_empty_box = [False]

    def add_p(p):
        if not _include_param(p, include_family):
            return
        st = p.StorageType
        if st != storage:
            return
        try:
            if st == StorageType.String:
                sval = p.AsString()
                if (sval is None) or (sval == u''):
                    has_empty_box[0] = True
                else:
                    if sval not in values:
                        values[sval] = sval
            elif st == StorageType.Integer:
                if not getattr(p, 'HasValue', True):
                    has_empty_box[0] = True
                else:
                    ival = p.AsInteger()
                    disp = p.AsValueString() if p.AsValueString() else unicode(ival)
                    if disp not in values:
                        values[disp] = ival
            elif st == StorageType.Double:
                if not getattr(p, 'HasValue', True):
                    has_empty_box[0] = True
                else:
                    dval = p.AsDouble()
                    disp = p.AsValueString() if p.AsValueString() else unicode(dval)
                    if disp not in values:
                        values[disp] = dval
            elif st == StorageType.ElementId:
                eid = p.AsElementId()
                if eid == ElementId.InvalidElementId:
                    has_empty_box[0] = True
                else:
                    disp = _eid_to_disp(eid)
                    if disp not in values:
                        values[disp] = eid
            else:
                has_empty_box[0] = True
        except:
            pass

    out = script.get_output()
    try:
        pb = out.create_progress_bar(len(elems), title=u'Собираю значения: {0}'.format(pname))
    except:
        pb = None

    try:
        for el in elems:
            if pb: pb.update()
            p = el.LookupParameter(pname)
            if p: add_p(p)
            if include_type and (p is None or getattr(p, 'StorageType', None) != storage):
                try:
                    typ = doc.GetElement(el.GetTypeId())
                    if typ:
                        pt = typ.LookupParameter(pname)
                        if pt: add_p(pt)
                except:
                    pass
    finally:
        if pb: pb.close()

    disp_list = list(values.keys())
    try:
        disp_list.sort(key=lambda x: _to_unicode(x).lower())
    except:
        disp_list.sort()

    return disp_list, values, has_empty_box[0]

def get_param(elem, pname, lookup_in_type=True):
    p = elem.LookupParameter(pname)
    if p is None and lookup_in_type:
        try:
            et = doc.GetElement(elem.GetTypeId())
            if et:
                p = et.LookupParameter(pname)
        except:
            p = None
    return p

def match_condition(elem, pname, op, raw_value, lookup_in_type):
    p = get_param(elem, pname, lookup_in_type)
    if op == u'пусто':
        return (p is None) or (not getattr(p, 'HasValue', True)) or                (p.StorageType == StorageType.String and (p.AsString() in (None, u''))) or                (p.StorageType == StorageType.ElementId and p.AsElementId() == ElementId.InvalidElementId)
    if op == u'не пусто':
        return not match_condition(elem, pname, u'пусто', None, lookup_in_type)

    if p is None:
        return False

    st = p.StorageType
    try:
        if st == StorageType.String:
            left = p.AsString()
            if left is None:
                left = p.AsValueString()
            eq = _to_unicode(left or u'') == _to_unicode(raw_value or u'')
            return eq if op == u'=' else (not eq)
        if st == StorageType.Integer:
            eq = int(p.AsInteger()) == int(raw_value)
            return eq if op == u'=' else (not eq)
        if st == StorageType.Double:
            left = float(p.AsDouble())
            right = float(raw_value)
            eq = abs(left - right) < 1e-9
            return eq if op == u'=' else (not eq)
        if st == StorageType.ElementId:
            left = p.AsElementId()
            if not isinstance(raw_value, ElementId):
                return False
            eq = (left == raw_value)
            return eq if op == u'=' else (not eq)
    except:
        return False
    return False

# ---------------- WinForms UI ----------------
clr.AddReference('System.Windows.Forms')
clr.AddReference('System.Drawing')
from System.Windows.Forms import (Form, Label, ComboBox, CheckBox, Button, DialogResult,
                                 FormBorderStyle, ComboBoxStyle, AutoCompleteMode, AutoCompleteSource)
from System.Drawing import Size, Point

OPS = [u'=', u'!=', u'пусто', u'не пусто']

class FilterForm(Form):
    def __init__(self):
        self.Text = u'Суперфильтр (3D/планы/разрезы)'
        self.FormBorderStyle = FormBorderStyle.FixedDialog
        self.MaximizeBox = False
        self.MinimizeBox = False
        self.ClientSize = Size(830, 470)

        y = 10
        self.lblInfo = Label()
        self.lblInfo.Text = u'Включены: системные, shared и ПАРАМЕТРЫ ПРОЕКТА. Семейст. (не shared) — по кнопке. Камеры исключены.'
        self.lblInfo.Location = Point(10, y)
        self.lblInfo.Size = Size(810, 20)
        self.Controls.Add(self.lblInfo)

        y += 28
        self.cbOnlyVis = CheckBox()
        self.cbOnlyVis.Text = u'Только видимые на активном виде'
        self.cbOnlyVis.Checked = True
        self.cbOnlyVis.Location = Point(10, y)
        self.cbOnlyVis.Size = Size(300, 20)
        self.cbOnlyVis.CheckedChanged += self._on_scope_changed
        self.Controls.Add(self.cbOnlyVis)

        self.cbIncludeTypes = CheckBox()
        self.cbIncludeTypes.Text = u'Включать параметры типов'
        self.cbIncludeTypes.Checked = True
        self.cbIncludeTypes.Location = Point(330, y)
        self.cbIncludeTypes.Size = Size(180, 20)
        self.cbIncludeTypes.CheckedChanged += self._on_scope_changed
        self.Controls.Add(self.cbIncludeTypes)

        self.btnLoadFamily = Button()
        self.btnLoadFamily.Text = u'Загрузить параметры семейства'
        self.btnLoadFamily.Location = Point(520, y-2)
        self.btnLoadFamily.Size = Size(250, 24)
        self.btnLoadFamily.Click += self._on_load_family
        self.Controls.Add(self.btnLoadFamily)

        y += 32
        self.lblLogic = Label()
        self.lblLogic.Text = u'Логика:'
        self.lblLogic.Location = Point(520, y+2)
        self.lblLogic.Size = Size(50, 18)
        self.Controls.Add(self.lblLogic)

        self.cmbLogic = ComboBox()
        self.cmbLogic.Items.Add(u'ИЛИ')
        self.cmbLogic.Items.Add(u'И')
        self.cmbLogic.SelectedIndex = 0
        self.cmbLogic.DropDownStyle = ComboBoxStyle.DropDownList
        self.cmbLogic.Location = Point(575, y)
        self.cmbLogic.Size = Size(80, 21)
        self.Controls.Add(self.cmbLogic)

        y += 34
        self.rows = []
        for i in range(5):
            self._add_row(i+1, y)
            y += 48

        self.btnOk = Button()
        self.btnOk.Text = u'Фильтровать'
        self.btnOk.Location = Point(590, self.ClientSize.Height-40)
        self.btnOk.Size = Size(110, 26)
        self.btnOk.Click += self._on_ok
        self.AcceptButton = self.btnOk
        self.Controls.Add(self.btnOk)

        self.btnCancel = Button()
        self.btnCancel.Text = u'Отмена'
        self.btnCancel.Location = Point(710, self.ClientSize.Height-40)
        self.btnCancel.Size = Size(100, 26)
        self.btnCancel.DialogResult = DialogResult.Cancel
        self.CancelButton = self.btnCancel
        self.Controls.Add(self.btnCancel)

        self.include_family = False
        self.param_names = []
        self.param_index = {}
        self.values_cache = {}
        self.values_loaded = set()

        self._rebuild_index()

    def _add_row(self, idx, y):
        lbl = Label()
        lbl.Text = u'Условие %d:' % idx
        lbl.Location = Point(10, y+6)
        lbl.Size = Size(80, 20)
        self.Controls.Add(lbl)

        cmbP = ComboBox()
        cmbP.DropDownStyle = ComboBoxStyle.DropDown
        cmbP.AutoCompleteMode = AutoCompleteMode.SuggestAppend
        cmbP.AutoCompleteSource = AutoCompleteSource.ListItems
        cmbP.Location = Point(95, y)
        cmbP.Size = Size(330, 22)
        cmbP.SelectedIndexChanged += self._on_param_changed
        cmbP.Tag = idx-1
        self.Controls.Add(cmbP)

        cmbO = ComboBox()
        for o in OPS:
            cmbO.Items.Add(o)
        cmbO.SelectedIndex = 0
        cmbO.DropDownStyle = ComboBoxStyle.DropDownList
        cmbO.Location = Point(430, y)
        cmbO.Size = Size(120, 22)
        cmbO.SelectedIndexChanged += self._on_op_changed
        cmbO.Tag = idx-1
        self.Controls.Add(cmbO)

        cmbV = ComboBox()
        cmbV.DropDownStyle = ComboBoxStyle.DropDownList
        cmbV.Location = Point(555, y)
        cmbV.Size = Size(245, 22)
        cmbV.DropDown += self._on_value_dropdown
        cmbV.Tag = idx-1
        self.Controls.Add(cmbV)

        self.rows.append((cmbP, cmbO, cmbV))

    def _on_scope_changed(self, sender, args):
        self._rebuild_index()

    def _on_load_family(self, sender, args):
        self.include_family = True
        self.btnLoadFamily.Enabled = False
        self._rebuild_index()

    def _rebuild_index(self):
        self.values_cache.clear()
        self.values_loaded.clear()
        only_vis = bool(self.cbOnlyVis.Checked)
        include_types = bool(self.cbIncludeTypes.Checked)
        names, pindex = build_param_index(only_vis, include_types, self.include_family, sample_per_cat=20)
        self.param_names = names
        self.param_index = pindex

        for cmbP, cmbO, cmbV in self.rows:
            cmbP.Items.Clear()
            for n in self.param_names:
                cmbP.Items.Add(n)
            cmbP.SelectedIndex = -1
            cmbV.Items.Clear()

    def _on_param_changed(self, sender, args):
        row = int(sender.Tag)
        cmbP, cmbO, cmbV = self.rows[row]
        cmbV.Items.Clear()
        pname = _to_unicode(cmbP.Text)
        if pname and pname in self.values_cache:
            for v in self.values_cache[pname]['disp_list']:
                cmbV.Items.Add(v)
        self._sync_value_enabled(row)

    def _on_value_dropdown(self, sender, args):
        row = int(sender.Tag)
        cmbP, cmbO, cmbV = self.rows[row]
        pname = _to_unicode(cmbP.Text)
        if not pname:
            return
        if pname in self.values_loaded:
            return
        only_vis = bool(self.cbOnlyVis.Checked)
        include_types = bool(self.cbIncludeTypes.Checked)
        storage = self.param_index.get(pname, None)
        if storage is None:
            return
        disp_list, mapping, has_empty = collect_values_for_param(
            pname, storage, only_vis, include_types, self.include_family
        )
        self.values_cache[pname] = {'disp_list': disp_list, 'map': mapping, 'has_empty': has_empty}
        self.values_loaded.add(pname)
        cmbV.Items.Clear()
        for v in disp_list:
            cmbV.Items.Add(v)

    def _on_op_changed(self, sender, args):
        row = int(sender.Tag)
        self._sync_value_enabled(row)

    def _sync_value_enabled(self, row):
        cmbP, cmbO, cmbV = self.rows[row]
        op = _to_unicode(cmbO.Text)
        need_val = op not in (u'пусто', u'не пусто')
        cmbV.Enabled = need_val

    def _on_ok(self, sender, args):
        conds = []
        for cmbP, cmbO, cmbV in self.rows:
            pname = _to_unicode(cmbP.Text).strip()
            op = _to_unicode(cmbO.Text).strip()
            if not pname:
                continue
            if op in (u'пусто', u'не пусто'):
                raw = None
            else:
                disp = _to_unicode(cmbV.Text)
                if disp == u'' or disp is None:
                    continue
                cache = self.values_cache.get(pname)
                if cache is None:
                    storage = self.param_index.get(pname, None)
                    only_vis = bool(self.cbOnlyVis.Checked)
                    include_types = bool(self.cbIncludeTypes.Checked)
                    disp_list, mapping, has_empty = collect_values_for_param(
                        pname, storage, only_vis, include_types, self.include_family
                    )
                    cache = {'disp_list': disp_list, 'map': mapping, 'has_empty': has_empty}
                    self.values_cache[pname] = cache
                raw = cache['map'].get(disp, disp)
            conds.append((pname, op, raw))

        if not conds:
            from pyrevit import forms
            forms.alert(u'Не выбрано ни одного условия.', title=u'Суперфильтр')
            return

        self.values = {
            'conditions': conds,
            'onlyvis': bool(self.cbOnlyVis.Checked),
            'search_types': True,
            'logic': _to_unicode(self.cmbLogic.Text)
        }
        self.DialogResult = DialogResult.OK
        self.Close()

# ---------------- Main ----------------
def main():
    ensure_view_supported(active_view)
    frm = FilterForm()
    res = frm.ShowDialog()
    if res != DialogResult.OK or not getattr(frm, 'values', None):
        return
    ui = frm.values

    conditions = ui['conditions']
    only_vis = ui['onlyvis']
    lookup_in_type = ui['search_types']
    use_or = (ui['logic'] == u'ИЛИ')

    elems = collect_candidates(only_vis)
    matched_ids = []

    out = script.get_output()
    try:
        pb = out.create_progress_bar(len(elems), title=u'Фильтрую элементы...')
    except:
        pb = None

    try:
        for el in elems:
            if pb: pb.update()
            if _is_camera(el):
                continue
            flags = []
            for (pname, op, raw) in conditions:
                ok = match_condition(el, pname, op, raw, lookup_in_type)
                flags.append(ok)
                if use_or and ok:
                    break
                if (not use_or) and (not ok):
                    break
            res = any(flags) if use_or else all(flags)
            if res:
                matched_ids.append(el.Id)
    finally:
        if pb: pb.close()

    if not matched_ids:
        from pyrevit import forms
        forms.alert(u'Элементы не найдены по выбранным условиям.', title=u'Суперфильтр')
        return

    from System.Collections.Generic import List as CsList
    sel_ids = CsList[ElementId](matched_ids)
    uidoc.Selection.SetElementIds(sel_ids)
    try:
        uidoc.ShowElements(sel_ids)
    except InvalidOperationException:
        pass

    from pyrevit import forms
    forms.alert(u'Найдено и выбрано элементов: {0}'.format(len(matched_ids)),
                title=u'Суперфильтр', warn_icon=False)

if __name__ == '__main__':
    main()
