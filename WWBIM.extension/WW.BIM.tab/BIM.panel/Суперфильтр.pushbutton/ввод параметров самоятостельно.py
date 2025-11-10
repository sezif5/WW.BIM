# -*- coding: utf-8 -*-
# SuperFilter 3D for pyRevit (IronPython)
# Версия: WinForms v1.2 (ComboBoxStyle enum fix)
from __future__ import print_function, division

import clr

from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId, StorageType, View3D,
    UnitUtils, CategoryType
)
try:
    from Autodesk.Revit.DB import UnitTypeId  # Revit 2021+
except:
    UnitTypeId = None

from Autodesk.Revit.Exceptions import InvalidOperationException
from pyrevit import revit, script

doc = revit.doc
uidoc = revit.uidoc
active_view = doc.ActiveView

def ensure_view3d(view):
    if not isinstance(view, View3D) or view.IsTemplate:
        from pyrevit import forms
        forms.alert(u'Активируйте обычный 3D-вид (не шаблон) и запустите снова.', title=u'Суперфильтр 3D')
        raise SystemExit

def _to_unicode(s):
    try:
        return unicode(s)
    except:
        try:
            return s.decode('utf-8')
        except:
            return s

def get_param(elem, pname, lookup_in_type=True):
    if not pname:
        return None
    p = elem.LookupParameter(pname)
    if p is None and lookup_in_type:
        try:
            et = doc.GetElement(elem.GetTypeId())
            if et:
                p = et.LookupParameter(pname)
        except:
            p = None
    return p

def is_empty_param(param):
    if param is None:
        return True
    st = param.StorageType
    if st == StorageType.String:
        s = param.AsString()
        if s is None or s == u'':
            vs = param.AsValueString()
            return (vs is None) or (vs == u'')
        return False
    elif st == StorageType.ElementId:
        return param.AsElementId() == ElementId.InvalidElementId
    elif st == StorageType.Integer:
        return False
    elif st == StorageType.Double:
        return False
    return True

def units_to_internal(num_value, param):
    try:
        spec = param.Definition.GetSpecTypeId()
        fmt = doc.GetUnits().GetFormatOptions(spec)
        utid = fmt.GetUnitTypeId()
        return UnitUtils.ConvertToInternalUnits(num_value, utid)
    except:
        try:
            dut = param.DisplayUnitType
            return UnitUtils.ConvertToInternalUnits(num_value, dut)
        except:
            return num_value

def parse_value_for_param(text, param):
    if param is None:
        return None
    st = param.StorageType
    if st == StorageType.String:
        return _to_unicode(text or u'')
    if st == StorageType.Integer:
        t = _to_unicode((text or u'')).strip().lower()
        if t in (u'true', u'1', u'да', u'yes', u'y', u'истина', u'on'):
            return 1
        if t in (u'false', u'0', u'нет', u'no', u'n', u'ложь', u'off'):
            return 0
        try:
            return int(float(t.replace(u',', u'.')))
        except:
            return None
    if st == StorageType.Double:
        if text is None or _to_unicode(text).strip() == u'':
            return None
        try:
            num = float(_to_unicode(text).replace(u',', u'.'))
        except:
            return None
        return units_to_internal(num, param)
    if st == StorageType.ElementId:
        try:
            return ElementId(int(float(_to_unicode(text).replace(u',', u'.'))))
        except:
            return ElementId.InvalidElementId
    return None

def cmp_string(lhs, op, rhs, ignore_case=True):
    lhs = _to_unicode(lhs or u'')
    rhs = _to_unicode(rhs or u'')
    if ignore_case:
        lhs, rhs = lhs.lower(), rhs.lower()
    if op == u'=':
        return lhs == rhs
    if op == u'!=':
        return lhs != rhs
    if op == u'содержит':
        return rhs in lhs
    if op == u'начинается с':
        return lhs.startswith(rhs)
    if op == u'заканчивается на':
        return lhs.endswith(rhs)
    return False

def cmp_number(lhs, op, rhs):
    try:
        if op == u'=':
            return abs(lhs - rhs) < 1e-9
        if op == u'!=':
            return abs(lhs - rhs) >= 1e-9
        if op == u'>':
            return lhs > rhs
        if op == u'>=':
            return lhs >= rhs
        if op == u'<':
            return lhs < rhs
        if op == u'<=':
            return lhs <= rhs
    except:
        return False
    return False

def match_condition(elem, pname, op, raw_value, lookup_in_type, ignore_case=True):
    p = get_param(elem, pname, lookup_in_type)
    if op == u'пусто':
        return is_empty_param(p)
    if op == u'не пусто':
        return not is_empty_param(p)
    if p is None:
        return False

    st = p.StorageType
    if st == StorageType.String:
        left = p.AsString()
        if left is None:
            left = p.AsValueString()
        return cmp_string(left, op, raw_value, ignore_case)

    if st == StorageType.Integer:
        left = p.AsInteger()
        right = parse_value_for_param(raw_value, p)
        if right is None:
            return False
        return cmp_number(left, op, right)

    if st == StorageType.Double:
        left = p.AsDouble()
        right = parse_value_for_param(raw_value, p)
        if right is None:
            return False
        return cmp_number(left, op, right)

    if st == StorageType.ElementId:
        left = p.AsElementId()
        right = parse_value_for_param(raw_value, p)
        if op == u'=':
            return left == right
        if op == u'!=':
            return left != right
        return False

    return False

# ---------- WinForms UI ----------
clr.AddReference('System.Windows.Forms')
clr.AddReference('System.Drawing')
from System.Windows.Forms import (Form, Label, TextBox, ComboBox, CheckBox, Button,
                                 DialogResult, FormBorderStyle, ComboBoxStyle)
from System.Drawing import Size, Point

OPS = [u'=', u'!=', u'содержит', u'начинается с', u'заканчивается на', u'>', u'>=', u'<', u'<=', u'пусто', u'не пусто']

class FilterForm(Form):
    def __init__(self):
        self.Text = u'Суперфильтр 3D (WinForms)'
        self.FormBorderStyle = FormBorderStyle.FixedDialog
        self.MaximizeBox = False
        self.MinimizeBox = False
        self.ClientSize = Size(620, 410)

        y = 10
        self.lblInfo = Label()
        self.lblInfo.Text = u'Выбор на активном 3D-виде. Пустые строки игнорируются.'
        self.lblInfo.Location = Point(10, y)
        self.lblInfo.Size = Size(600, 20)
        self.Controls.Add(self.lblInfo)

        y += 28
        self.cbOnlyVis = CheckBox()
        self.cbOnlyVis.Text = u'Только видимые на активном 3D-виде'
        self.cbOnlyVis.Checked = True
        self.cbOnlyVis.Location = Point(10, y)
        self.cbOnlyVis.Size = Size(350, 20)
        self.Controls.Add(self.cbOnlyVis)

        self.cbLookupType = CheckBox()
        self.cbLookupType.Text = u'Если у экземпляра нет параметра — искать в типе'
        self.cbLookupType.Checked = True
        self.cbLookupType.Location = Point(320, y)
        self.cbLookupType.Size = Size(290, 20)
        self.Controls.Add(self.cbLookupType)

        y += 26
        self.cbIgnoreCase = CheckBox()
        self.cbIgnoreCase.Text = u'Игнорировать регистр для строк'
        self.cbIgnoreCase.Checked = True
        self.cbIgnoreCase.Location = Point(10, y)
        self.cbIgnoreCase.Size = Size(260, 20)
        self.Controls.Add(self.cbIgnoreCase)

        self.lblLogic = Label()
        self.lblLogic.Text = u'Логика:'
        self.lblLogic.Location = Point(320, y+2)
        self.lblLogic.Size = Size(60, 18)
        self.Controls.Add(self.lblLogic)

        self.cmbLogic = ComboBox()
        self.cmbLogic.Items.Add(u'ИЛИ')
        self.cmbLogic.Items.Add(u'И')
        self.cmbLogic.SelectedIndex = 0
        self.cmbLogic.DropDownStyle = ComboBoxStyle.DropDownList
        self.cmbLogic.Location = Point(380, y)
        self.cmbLogic.Size = Size(80, 21)
        self.Controls.Add(self.cmbLogic)

        y += 30
        self.rows = []
        for i in range(5):
            self._add_row(i+1, y)
            y += 38

        self.btnOk = Button()
        self.btnOk.Text = u'Фильтровать'
        self.btnOk.Location = Point(390, self.ClientSize.Height-40)
        self.btnOk.Size = Size(100, 26)
        self.btnOk.Click += self._on_ok
        self.AcceptButton = self.btnOk
        self.Controls.Add(self.btnOk)

        self.btnCancel = Button()
        self.btnCancel.Text = u'Отмена'
        self.btnCancel.Location = Point(500, self.ClientSize.Height-40)
        self.btnCancel.Size = Size(100, 26)
        self.btnCancel.DialogResult = DialogResult.Cancel
        self.CancelButton = self.btnCancel
        self.Controls.Add(self.btnCancel)

        self.values = None

    def _add_row(self, idx, y):
        lbl = Label()
        lbl.Text = u'Условие %d:' % idx
        lbl.Location = Point(10, y+4)
        lbl.Size = Size(70, 20)
        self.Controls.Add(lbl)

        tb = TextBox()
        tb.Location = Point(85, y)
        tb.Size = Size(220, 22)
        self.Controls.Add(tb)

        cmb = ComboBox()
        for o in OPS:
            cmb.Items.Add(o)
        cmb.SelectedIndex = 2  # 'содержит'
        cmb.DropDownStyle = ComboBoxStyle.DropDownList
        cmb.Location = Point(310, y)
        cmb.Size = Size(130, 22)
        self.Controls.Add(cmb)

        val = TextBox()
        val.Location = Point(445, y)
        val.Size = Size(155, 22)
        self.Controls.Add(val)

        self.rows.append((tb, cmb, val))

    def _on_ok(self, sender, args):
        conds = []
        for tb, cmb, val in self.rows:
            pname = _to_unicode(tb.Text).strip()
            op = _to_unicode(cmb.Text).strip()
            v = _to_unicode(val.Text)
            if pname:
                conds.append((pname, op, v))

        if not conds:
            from pyrevit import forms
            forms.alert(u'Не задано ни одного условия.', title=u'Суперфильтр 3D')
            return

        self.values = {
            'conditions': conds,
            'onlyvis': bool(self.cbOnlyVis.Checked),
            'search_types': bool(self.cbLookupType.Checked),
            'ignore_case': bool(self.cbIgnoreCase.Checked),
            'logic': _to_unicode(self.cmbLogic.Text)
        }
        self.DialogResult = DialogResult.OK
        self.Close()

def run_ui():
    frm = FilterForm()
    res = frm.ShowDialog()
    if res == DialogResult.OK and frm.values:
        return frm.values
    else:
        raise SystemExit

def collect_candidates(only_visible=True):
    if only_visible:
        col = FilteredElementCollector(doc, active_view.Id)
    else:
        col = FilteredElementCollector(doc)
    col = col.WhereElementIsNotElementType()
    result = []
    for el in col:
        cat = el.Category
        if cat and cat.CategoryType == CategoryType.Model:
            result.append(el)
    return result

def main():
    ensure_view3d(active_view)
    ui = run_ui()

    conditions = ui['conditions']
    only_vis = ui['onlyvis']
    lookup_in_type = ui['search_types']
    ignore_case = ui['ignore_case']
    use_or = (ui['logic'] == u'ИЛИ')

    elems = collect_candidates(only_vis)
    matched_ids = []

    out = script.get_output()
    try:
        pb = out.create_progress_bar(len(elems), title=u'Фильтрация элементов...')
    except:
        pb = None

    try:
        for el in elems:
            if pb: pb.update()
            flags = []
            for (pname, op, val) in conditions:
                ok = match_condition(el, pname, op, val, lookup_in_type, ignore_case)
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
        forms.alert(u'Элементы не найдены по заданным условиям.', title=u'Суперфильтр 3D')
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
                title=u'Суперфильтр 3D', warn_icon=False)

if __name__ == '__main__':
    main()
