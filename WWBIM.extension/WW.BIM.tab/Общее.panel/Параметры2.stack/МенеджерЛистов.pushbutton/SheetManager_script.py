# -*- coding: utf-8 -*-
# pyRevit button script: Менеджер листов (v23 — редактирование прямо в таблице видимое)

import clr
import re

# --- AddReferences ДО импортов .NET ---
clr.AddReference('System')
clr.AddReference('System.Windows.Forms')
clr.AddReference('System.Drawing')
clr.AddReference('RevitAPI')
clr.AddReference('RevitServices')
clr.AddReference('RevitAPIUI')

from System import Array, String
from System.Windows.Forms import (Form, ComboBox, Label, Button,
                                  ListView, ColumnHeader, AnchorStyles,
                                  View, ListViewItem, BorderStyle, FormStartPosition,
                                  ComboBoxStyle, MessageBox, MessageBoxButtons, MessageBoxIcon,
                                  NumericUpDown, Keys)
from System.Drawing import Size, Point, Rectangle, Color

from Autodesk.Revit.DB import (FilteredElementCollector, ViewSheet, BuiltInParameter, BuiltInCategory,
                               Transaction, TransactionGroup, IFailuresPreprocessor,
                               FailureProcessingResult, ElementId, StorageType)

# --- Значения по умолчанию ---
DEFAULT_GROUP_PARAM = u"ADSK_Штамп Раздел проекта"
DEFAULT_NUM_PARAM   = u"DVLK_Штамп_Номер листа"
FORMAT_PARAM_CYR    = u"А"    # Кириллическая 'А'
FORMAT_PARAM_LAT    = u"A"    # Латинская 'A'

# --- Доступ к документу ---
uidoc = __revit__.ActiveUIDocument
doc = uidoc.Document

# --- Helpers ---
def get_param_str(elem, name):
    p = elem.LookupParameter(name)
    if p:
        try:
            return (p.AsString() or u"").strip()
        except:
            pass
    return u""

def set_param_str(elem, name, value):
    p = elem.LookupParameter(name)
    if p and not p.IsReadOnly:
        p.Set(value)
        return True
    return False

def get_sheet_number(sheet):
    p = sheet.get_Parameter(BuiltInParameter.SHEET_NUMBER)
    return ((p.AsString() if p else u"") or u"").strip()

def set_sheet_number(sheet, value):
    p = sheet.get_Parameter(BuiltInParameter.SHEET_NUMBER)
    if p and not p.IsReadOnly:
        p.Set(value)
        return True
    return False

def is_placeholder(sheet):
    try:
        return sheet.IsPlaceholder
    except:
        return False

def get_sheets(document):
    return [s for s in FilteredElementCollector(document).OfClass(ViewSheet).ToElements() if not is_placeholder(s)]

def get_titleblocks_on_sheet(sheet):
    col = FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_TitleBlocks).WhereElementIsNotElementType()
    res = []
    for tb in col:
        try:
            if tb.OwnerViewId == sheet.Id:
                res.append(tb)
        except:
            pass
    return res

def get_format_value(sheet):
    tblks = get_titleblocks_on_sheet(sheet)
    if not tblks:
        return u""
    tb = tblks[0]
    p = tb.LookupParameter(FORMAT_PARAM_CYR) or tb.LookupParameter(FORMAT_PARAM_LAT)
    if not p:
        return u""
    try:
        if p.StorageType == StorageType.Integer:
            return unicode(p.AsInteger())
        else:
            return (p.AsString() or u"")
    except:
        return u""

def set_format_value(sheet, value_text):
    tblks = get_titleblocks_on_sheet(sheet)
    if not tblks:
        return False
    ok_any = False
    for tb in tblks:
        p = tb.LookupParameter(FORMAT_PARAM_CYR) or tb.LookupParameter(FORMAT_PARAM_LAT)
        if not p or p.IsReadOnly:
            continue
        try:
            if p.StorageType == StorageType.Integer:
                ival = int(value_text)
                p.Set(ival)
                ok_any = True
            else:
                p.Set(value_text)
                ok_any = True
        except:
            pass
    return ok_any

_num_re = re.compile(r"(\d+)")
def natural_key(s):
    if s is None:
        s = u""
    parts = _num_re.split(s)
    key = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            key.append(part)
        else:
            try:
                key.append(int(part))
            except:
                key.append(part)
    return tuple(key)

# --- Failures suppressor (минимальный) ---
class SuppressErrors(IFailuresPreprocessor):
    def PreprocessFailures(self, fa):
        return FailureProcessingResult.Continue

def with_suppress(t):
    fho = t.GetFailureHandlingOptions()
    fho = fho.SetFailuresPreprocessor(SuppressErrors())
    t.SetFailureHandlingOptions(fho)

# --- Модель строки ---
class SheetRow(object):
    __slots__ = ("sheet_id", "sheetnum", "name")
    def __init__(self, sheet):
        self.sheet_id = sheet.Id.IntegerValue
        self.sheetnum = get_sheet_number(sheet) or u""
        self.name = sheet.Name or u""

# --- Сбор имён параметров ---
def collect_string_param_names(sheets):
    names_all = set()
    names_writable = set()
    for s in sheets:
        try:
            for p in s.Parameters:
                d = p.Definition
                if d is None:
                    continue
                nm = (d.Name or u"").strip()
                if not nm:
                    continue
                try:
                    _ = p.AsString()
                    names_all.add(nm)
                    if not p.IsReadOnly:
                        names_writable.add(nm)
                except:
                    pass
        except:
            pass
    return sorted(names_all, key=natural_key), sorted(names_writable, key=natural_key)

# --- Форма ---
class SheetManagerForm(Form):
    def __init__(self):
        self.Text = u"Менеджер листов | Нумерация"
        self.MinimumSize = Size(980, 640)
        self.Size = Size(1040, 680)
        self.StartPosition = FormStartPosition.CenterScreen

        # Контролы параметров
        self.lblSortParam = Label(); self.lblSortParam.Text = u"Параметр сортировки:"; self.lblSortParam.AutoSize = True; self.lblSortParam.Location = Point(12, 14)
        self.cmbSortParam = ComboBox(); self.cmbSortParam.DropDownStyle = ComboBoxStyle.DropDownList; self.cmbSortParam.Location = Point(160, 10); self.cmbSortParam.Width = 220; self.cmbSortParam.Anchor = AnchorStyles.Top | AnchorStyles.Left

        self.lblGroupVal  = Label(); self.lblGroupVal.Text = u"Альбом:"; self.lblGroupVal.AutoSize = True; self.lblGroupVal.Location = Point(390, 14)
        self.cmbGroupVal  = ComboBox(); self.cmbGroupVal.DropDownStyle = ComboBoxStyle.DropDownList; self.cmbGroupVal.Location = Point(460, 10); self.cmbGroupVal.Width = 220; self.cmbGroupVal.Anchor = AnchorStyles.Top | AnchorStyles.Left

        self.lblStart  = Label(); self.lblStart.Text = u"Начинать с:"; self.lblStart.AutoSize = True; self.lblStart.Location = Point(460, 40)
        self.numStart  = NumericUpDown(); self.numStart.Minimum = 1; self.numStart.Maximum = 9999; self.numStart.Value = 1; self.numStart.Width = 80; self.numStart.Location = Point(540, 38)

        self.lblNumParam  = Label(); self.lblNumParam.Text = u"Параметр нумерации:"; self.lblNumParam.AutoSize = True; self.lblNumParam.Location = Point(12, 44)
        self.cmbNumParam  = ComboBox(); self.cmbNumParam.DropDownStyle = ComboBoxStyle.DropDownList; self.cmbNumParam.Location = Point(160, 40); self.cmbNumParam.Width = 220; self.cmbNumParam.Anchor = AnchorStyles.Top | AnchorStyles.Left

        # Таблица
        self.lv = ListView(); self.lv.View = View.Details; self.lv.FullRowSelect = True; self.lv.MultiSelect = False; self.lv.BorderStyle = BorderStyle.FixedSingle
        self.lv.Location = Point(12, 100); self.lv.Size = Size(900, 520); self.lv.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right

        ch1 = ColumnHeader(); ch1.Text = u"Порядок"; ch1.Width = 70
        ch2 = ColumnHeader(); ch2.Text = u"Сист. номер"; ch2.Width = 190
        ch3 = ColumnHeader(); ch3.Text = u"Имя листа"; ch3.Width = 390
        ch4 = ColumnHeader(); ch4.Text = DEFAULT_NUM_PARAM; ch4.Width = 150
        ch5 = ColumnHeader(); ch5.Text = u"Формат (A)"; ch5.Width = 90
        self.col_num = ch4
        self.col_fmt = ch5
        self.lv.Columns.AddRange(Array[ColumnHeader]([ch1, ch2, ch3, ch4, ch5]))

        # Правая колонка кнопок
        self.btnUp   = Button(); self.btnUp.Text = u"▲ Вверх"; self.btnUp.Size = Size(110, 34)
        self.btnDown = Button(); self.btnDown.Text = u"▼ Вниз";  self.btnDown.Size = Size(110, 34)
        self.btnCopy = Button(); self.btnCopy.Text = u"Копировать"; self.btnCopy.Size = Size(110, 34)
        self.btnNumerate = Button(); self.btnNumerate.Text = u"Нумеровать"; self.btnNumerate.Size = Size(120, 34)
        self.btnCancel   = Button(); self.btnCancel.Text = u"Закрыть";    self.btnCancel.Size = Size(110, 34)

        for b in [self.btnUp, self.btnDown, self.btnCopy]:
            b.Anchor = AnchorStyles.Top | AnchorStyles.Right
        for c in [self.btnNumerate, self.btnCancel]:
            c.Anchor = AnchorStyles.Bottom | AnchorStyles.Right

        for c in [self.lblSortParam, self.cmbSortParam, self.lblGroupVal, self.cmbGroupVal, self.lblStart, self.numStart,
                  self.lblNumParam, self.cmbNumParam, self.lv, self.btnUp, self.btnDown, self.btnCopy, self.btnNumerate, self.btnCancel]:
            self.Controls.Add(c)

        # Редактор: делаем дочерним контролом списка (чтобы точно был над сабайтем)
        from System.Windows.Forms import TextBox as WinTextBox
        self.editBox = WinTextBox()
        self.editBox.Visible = False
        self.editBox.BorderStyle = BorderStyle.FixedSingle
        self.editBox.BackColor = Color.White
        self.editBox.ForeColor = Color.Black
        self.editBox.Font = self.lv.Font
        self.editBox.KeyDown += self._editbox_keydown
        self.editBox.Leave += self._editbox_leave
        self.lv.Controls.Add(self.editBox)  # ключевое отличие
        self._edit_ctx = None

        # Данные
        self.sheets = [SheetRow(s) for s in get_sheets(doc)]
        self.sheet_map = {r.sheet_id: doc.GetElement(ElementId(r.sheet_id)) for r in self.sheets}

        names_all, names_writable = collect_string_param_names(self.sheet_map.values())
        if DEFAULT_GROUP_PARAM not in names_all:
            names_all.insert(0, DEFAULT_GROUP_PARAM)
        if DEFAULT_NUM_PARAM not in names_writable:
            names_writable.insert(0, DEFAULT_NUM_PARAM)
        for n in names_all:
            self.cmbSortParam.Items.Add(n)
        for n in names_writable:
            self.cmbNumParam.Items.Add(n)
        try:
            self.cmbSortParam.SelectedIndex = max(0, list(self.cmbSortParam.Items).IndexOf(DEFAULT_GROUP_PARAM))
        except:
            self.cmbSortParam.SelectedIndex = 0
        try:
            self.cmbNumParam.SelectedIndex = max(0, list(self.cmbNumParam.Items).IndexOf(DEFAULT_NUM_PARAM))
        except:
            self.cmbNumParam.SelectedIndex = 0

        # События
        self.cmbSortParam.SelectedIndexChanged += self.on_sort_param_changed
        self.cmbGroupVal.SelectedIndexChanged  += self.on_group_value_changed
        self.cmbNumParam.SelectedIndexChanged  += self.on_num_param_changed
        self.lv.MouseDoubleClick += self.on_lv_double_click
        self.lv.ColumnWidthChanged += self._hide_editor_event
        self.lv.SizeChanged += self._hide_editor_event
        self.lv.MouseWheel += self._hide_editor_event
        self.btnUp.Click += self.on_move_up
        self.btnDown.Click += self.on_move_down
        self.btnCopy.Click += self.on_copy_below
        self.btnNumerate.Click += self.on_numerate
        self.btnCancel.Click += self.on_cancel
        self.Resize += self.on_resize

        # Инициал
        self.order_ids = []
        self.current_rows = []
        self.rebuild_group_values()
        self.reload_rows(set_initial_order=True)
        self._layout_controls()

    # Размещение
    def _layout_controls(self):
        right_margin = 20
        bottom_margin = 20
        gap = 10
        client_w = self.ClientSize.Width
        client_h = self.ClientSize.Height
        x_right = client_w - right_margin - self.btnUp.Width
        y = self.lv.Top
        for b in [self.btnUp, self.btnDown, self.btnCopy]:
            b.Location = Point(x_right, y); y += b.Height + gap
        y_bottom = client_h - bottom_margin - self.btnNumerate.Height
        self.btnNumerate.Location = Point(client_w - right_margin - self.btnNumerate.Width, y_bottom)
        self.btnCancel.Location   = Point(self.btnNumerate.Left - gap - self.btnCancel.Width, y_bottom)
        new_lv_width = (x_right - gap) - self.lv.Left
        if new_lv_width > 260: self.lv.Width = new_lv_width
        new_lv_height = (y_bottom - gap) - self.lv.Top
        if new_lv_height > 180: self.lv.Height = new_lv_height
        # Переставить редактор если открыт
        if self.editBox.Visible and self._edit_ctx is not None:
            self._position_editor(self._edit_ctx['row'], self._edit_ctx['col'])

    def on_resize(self, sender, args):
        self._layout_controls()

    # Построение
    def rebuild_group_values(self):
        sort_param = self.cmbSortParam.SelectedItem if self.cmbSortParam.SelectedItem else DEFAULT_GROUP_PARAM
        values = set()
        for sid, sh in self.sheet_map.items():
            v = get_param_str(sh, sort_param)
            if v: values.add(v)
        values = sorted(list(values), key=natural_key)
        self.cmbGroupVal.Items.Clear()
        for v in values: self.cmbGroupVal.Items.Add(v)
        if self.cmbGroupVal.Items.Count > 0: self.cmbGroupVal.SelectedIndex = 0

    def _make_item(self, index, row, num_param):
        sh = self.sheet_map[row.sheet_id]
        numval = get_param_str(sh, num_param)
        fmtval = get_format_value(sh)
        arr = Array[String]([str(index), row.sheetnum, row.name, numval, fmtval])
        it = ListViewItem(arr); it.Tag = row.sheet_id
        return it

    def reload_rows(self, set_initial_order=False):
        sort_param = self.cmbSortParam.SelectedItem if self.cmbSortParam.SelectedItem else DEFAULT_GROUP_PARAM
        group_val  = self.cmbGroupVal.SelectedItem if self.cmbGroupVal.SelectedItem else u""
        num_param  = self.cmbNumParam.SelectedItem if self.cmbNumParam.SelectedItem else DEFAULT_NUM_PARAM
        rows = []
        for r in self.sheets:
            sh = self.sheet_map[r.sheet_id]
            if (get_param_str(sh, sort_param) or u"") == (group_val or u""):
                rows.append(r)
        if set_initial_order or not self.order_ids:
            any_num = any((get_param_str(self.sheet_map[r.sheet_id], num_param) or u"").strip() for r in rows)
            if any_num:
                def key_num(r):
                    nv = (get_param_str(self.sheet_map[r.sheet_id], num_param) or u"").strip()
                    try: return (0, int(nv))
                    except: return (1, natural_key(nv), natural_key(r.sheetnum))
                rows.sort(key=key_num)
            else:
                rows.sort(key=lambda r: natural_key(r.sheetnum))
            self.order_ids = [r.sheet_id for r in rows]
        else:
            id_to_row = {r.sheet_id: r for r in rows}
            rows = [id_to_row[i] for i in self.order_ids if i in id_to_row]
        self.current_rows = rows
        self.col_num.Text = self.cmbNumParam.SelectedItem if self.cmbNumParam.SelectedItem else DEFAULT_NUM_PARAM
        self.lv.BeginUpdate()
        try:
            self.lv.Items.Clear()
            for i, r in enumerate(self.current_rows, 1):
                self.lv.Items.Add(self._make_item(i, r, self.col_num.Text))
            if self.lv.Items.Count > 0: self.lv.Items[0].Selected = True
        finally:
            self.lv.EndUpdate()
        self._hide_editor()

    # События
    def on_sort_param_changed(self, sender, args):
        self.rebuild_group_values(); self.order_ids = []; self.reload_rows(set_initial_order=True)

    def on_group_value_changed(self, sender, args):
        self.order_ids = []; self.reload_rows(set_initial_order=True)

    def on_num_param_changed(self, sender, args):
        self.reload_rows(set_initial_order=False)

    def _selected_index(self):
        return int(self.lv.SelectedIndices[0]) if self.lv.SelectedIndices.Count > 0 else -1

    def on_move_up(self, sender, args):
        idx = self._selected_index()
        if idx <= 0: return
        self.current_rows[idx-1], self.current_rows[idx] = self.current_rows[idx], self.current_rows[idx-1]
        self.order_ids[idx-1], self.order_ids[idx] = self.order_ids[idx], self.order_ids[idx-1]
        self._rerender_keep_selection(idx-1)

    def on_move_down(self, sender, args):
        idx = self._selected_index()
        if idx < 0 or idx >= len(self.current_rows)-1: return
        self.current_rows[idx+1], self.current_rows[idx] = self.current_rows[idx], self.current_rows[idx+1]
        self.order_ids[idx+1], self.order_ids[idx] = self.order_ids[idx], self.order_ids[idx+1]
        self._rerender_keep_selection(idx+1)

    def on_copy_below(self, sender, args):
        idx = self._selected_index()
        if idx < 0 or idx >= len(self.current_rows): return
        src_row = self.current_rows[idx]
        src_sheet = self.sheet_map[src_row.sheet_id]

        # Тип титульника
        tblks = get_titleblocks_on_sheet(src_sheet)
        symbol_id = None
        if tblks:
            try: symbol_id = tblks[0].Symbol.Id
            except: symbol_id = None
        if symbol_id is None:
            types = list(FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_TitleBlocks).WhereElementIsElementType())
            if types: symbol_id = types[0].Id
        if symbol_id is None:
            MessageBox.Show(u"Не найден тип титульного листа для копирования.", u"Копирование", MessageBoxButtons.OK, MessageBoxIcon.Warning)
            return

        t = Transaction(doc, u"Копировать лист"); t.Start(); with_suppress(t)
        try:
            new_sheet = ViewSheet.Create(doc, symbol_id)
            try: new_sheet.Name = (src_sheet.Name or u"") + u" (копия)"
            except: pass
            sort_param = self.cmbSortParam.SelectedItem if self.cmbSortParam.SelectedItem else DEFAULT_GROUP_PARAM
            set_param_str(new_sheet, sort_param, get_param_str(src_sheet, sort_param))
            fmt = get_format_value(src_sheet)
            if fmt != u"": set_format_value(new_sheet, fmt)
        except Exception as ex:
            t.RollBack(); MessageBox.Show(u"Не удалось скопировать лист: " + unicode(ex), u"Копирование", MessageBoxButtons.OK, MessageBoxIcon.Error); return
        t.Commit()

        new_row = SheetRow(new_sheet)
        self.sheets.append(new_row)
        self.sheet_map[new_row.sheet_id] = new_sheet
        insert_at = idx + 1
        self.current_rows.insert(insert_at, new_row)
        self.order_ids.insert(insert_at, new_row.sheet_id)
        self._rerender_keep_selection(insert_at)

    # In-table edit mechanics
    def on_lv_double_click(self, sender, e):
        hit = self.lv.HitTest(e.Location)
        if hit is None or hit.Item is None or hit.SubItem is None:
            return
        item = hit.Item; sub = hit.SubItem
        row = item.Index
        # find col
        col = -1
        for i in range(item.SubItems.Count):
            if item.SubItems[i] == sub:
                col = i; break
        if col not in (2, 4):
            return
        self._start_edit(row, col, sub)

    def _position_editor(self, row, col):
        r = self.lv.Items[row].SubItems[col].Bounds
        self.editBox.Bounds = Rectangle(r.Left + 1, r.Top + 1, max(20, r.Width - 2), max(16, r.Height - 2))
        self.editBox.BringToFront()

    def _start_edit(self, row, col, sub):
        self._edit_ctx = {'row': row, 'col': col}
        self.editBox.Text = sub.Text
        self._position_editor(row, col)
        self.editBox.Visible = True
        self.editBox.Focus()
        self.editBox.SelectAll()

    def _hide_editor(self, *args):
        self.editBox.Visible = False
        self._edit_ctx = None

    def _hide_editor_event(self, sender, e):
        self._hide_editor()

    def _commit_editor(self):
        if not self.editBox.Visible or self._edit_ctx is not None and self._edit_ctx.get('row') is None:
            # defensive
            pass
        if not self.editBox.Visible or self._edit_ctx is None:
            return
        txt = (self.editBox.Text or u"").strip()
        row = self._edit_ctx['row']; col = self._edit_ctx['col']
        sid = self.current_rows[row].sheet_id
        sh = self.sheet_map[sid]

        if col == 2:
            if txt != (sh.Name or u""):
                t = Transaction(doc, u"Переименование листа"); t.Start(); with_suppress(t)
                try:
                    sh.Name = txt
                except Exception as ex:
                    t.RollBack(); MessageBox.Show(u"Не удалось переименовать: " + unicode(ex), u"Ошибка", MessageBoxButtons.OK, MessageBoxIcon.Error); return
                t.Commit()
                self.current_rows[row] = SheetRow(sh)
        elif col == 4:
            if txt != u"":
                try:
                    int(txt)
                except:
                    MessageBox.Show(u"Формат (A) должен быть целым числом.", u"Формат", MessageBoxButtons.OK, MessageBoxIcon.Warning)
                    return
                t = Transaction(doc, u"Формат A"); t.Start(); with_suppress(t)
                try:
                    ok = set_format_value(sh, txt)
                    if not ok:
                        MessageBox.Show(u"Параметр 'A' недоступен для записи на титульнике.", u"Формат", MessageBoxButtons.OK, MessageBoxIcon.Warning)
                except Exception as ex:
                    t.RollBack(); MessageBox.Show(u"Ошибка установки формата: " + unicode(ex), u"Формат", MessageBoxButtons.OK, MessageBoxIcon.Error); return
                t.Commit()

        self._rerender_keep_selection(row)
        self._hide_editor()

    def _editbox_keydown(self, sender, e):
        if e.KeyCode == Keys.Enter:
            self._commit_editor(); e.Handled = True
        elif e.KeyCode == Keys.Escape:
            self._hide_editor(); e.Handled = True

    def _editbox_leave(self, sender, e):
        self._commit_editor()

    def _rerender_keep_selection(self, new_index):
        num_param = self.cmbNumParam.SelectedItem if self.cmbNumParam.SelectedItem else DEFAULT_NUM_PARAM
        self.lv.BeginUpdate()
        try:
            self.lv.Items.Clear()
            for i, r in enumerate(self.current_rows, 1):
                self.lv.Items.Add(self._make_item(i, r, num_param))
            if 0 <= new_index < self.lv.Items.Count:
                self.lv.Items[new_index].Selected = True
                self.lv.Items[new_index].Focused = True
                self.lv.EnsureVisible(new_index)
        finally:
            self.lv.EndUpdate()

    # Нумерация
    def on_numerate(self, sender, args):
        if not self.current_rows: return
        sort_param = self.cmbSortParam.SelectedItem if self.cmbSortParam.SelectedItem else DEFAULT_GROUP_PARAM
        num_param  = self.cmbNumParam.SelectedItem if self.cmbNumParam.SelectedItem else DEFAULT_NUM_PARAM
        start_at   = int(self.numStart.Value)
        ids_in_order = list(self.order_ids)
        plan = []
        for idx, sid in enumerate(ids_in_order, 1):
            sh = self.sheet_map[sid]
            adsk = (get_param_str(sh, sort_param) or u"").strip()
            dvlk = unicode(start_at + idx - 1)
            tmp_num   = adsk + dvlk
            final_num = dvlk + u"_" + adsk
            plan.append((sh, dvlk, adsk, tmp_num, final_num))
        tg = TransactionGroup(doc, u"Нумерация листов"); tg.Start()
        t1 = Transaction(doc, u"Временные номера"); t1.Start(); with_suppress(t1)
        try:
            for (sh, dvlk, adsk, tmp_num, final_num) in plan: set_sheet_number(sh, tmp_num)
        except Exception as ex:
            t1.RollBack(); tg.RollBack(); MessageBox.Show(u"Ошибка на шаге временных номеров: " + unicode(ex), u"Ошибка", MessageBoxButtons.OK, MessageBoxIcon.Error); return
        t1.Commit()
        t2 = Transaction(doc, u"Финальные номера"); t2.Start(); with_suppress(t2)
        try:
            for (sh, dvlk, adsk, tmp_num, final_num) in plan:
                set_sheet_number(sh, final_num); set_param_str(sh, num_param, dvlk)
        except Exception as ex:
            t2.RollBack(); tg.RollBack(); MessageBox.Show(u"Ошибка на шаге финальных номеров: " + unicode(ex), u"Ошибка", MessageBoxButtons.OK, MessageBoxIcon.Error); return
        t2.Commit()
        tg.Assimilate()
        self.sheets = [SheetRow(s) for s in get_sheets(doc)]
        self.sheet_map = {r.sheet_id: doc.GetElement(ElementId(r.sheet_id)) for r in self.sheets}
        self.order_ids = []
        self.reload_rows(set_initial_order=True)

    def on_cancel(self, sender, args):
        self.Close()


# --- Запуск ---
form = SheetManagerForm()
form.ShowDialog()
