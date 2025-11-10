
# -*- coding: utf-8 -*-
# pyRevit button script: Batch add Shared Parameters to Families
# IronPython 2.7 (pyRevit), Revit 2020–2025
# v9: формулы для типов, значения по умолчанию для экземпляров (2 транзакции), фиксы Yes/No и диагностика

from __future__ import print_function, division

import sys, os, json, traceback
import clr

# .NET
clr.AddReference('System')
clr.AddReference('System.Core')
clr.AddReference('PresentationCore')
clr.AddReference('PresentationFramework')
clr.AddReference('WindowsBase')
clr.AddReference('System.Xml')
clr.AddReference('System.Windows.Forms')

import System
from System import Guid
from System.IO import StringReader
from System.Collections.ObjectModel import ObservableCollection
from System.Windows.Markup import XamlReader
from System.Xml import XmlReader
from System.Windows.Forms import FolderBrowserDialog, OpenFileDialog, DialogResult
from System.Collections.Generic import List, KeyValuePair

# Revit API
clr.AddReference('RevitAPI')
clr.AddReference('RevitAPIUI')
from Autodesk.Revit.DB import *
from Autodesk.Revit.UI import *

# pyRevit
from pyrevit import forms, script

logger = script.get_logger()
output = script.get_output()
uidoc = __revit__.ActiveUIDocument
app = __revit__.Application

# ---------------------------- Utilities ------------------------------------

def ensure_shared_parameters_def_file():
    sp_path = app.SharedParametersFilename
    if not sp_path or not os.path.exists(sp_path):
        ofd = OpenFileDialog()
        ofd.Filter = 'Shared Parameter file (*.txt)|*.txt|All files (*.*)|*.*'
        ofd.Title = u'Выберите файл общих параметров (.txt)'
        if ofd.ShowDialog() == DialogResult.OK:
            try:
                app.SharedParametersFilename = ofd.FileName
            except Exception as e:
                forms.alert(u'Не удалось назначить файл общих параметров:\n{}'.format(e), exitscript=True)
        else:
            return None
    dfile = app.OpenSharedParameterFile()
    if dfile is None:
        forms.alert(u'Не удалось открыть файл общих параметров: {}'.format(app.SharedParametersFilename), exitscript=True)
    return dfile

def all_bipg_options():
    items = []
    for name in dir(BuiltInParameterGroup):
        if name.startswith('PG_') or name == 'INVALID':
            try:
                enum_val = getattr(BuiltInParameterGroup, name)
                label = LabelUtils.GetLabelFor(enum_val)
                if label and label.strip():
                    items.append( (label, enum_val) )
            except:
                pass
    items.sort(key=lambda t: t[0].lower())
    return items

def walk_rfa_files(root, recursive=True):
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith('.rfa'):
                yield os.path.join(dirpath, fn)
        if not recursive:
            break

def get_existing_family_param_by_name(fm, name):
    for fp in fm.Parameters:
        try:
            if fp.Definition and fp.Definition.Name == name:
                return fp
        except:
            pass
    return None

def _ensure_family_has_type(fdoc):
    fm = fdoc.FamilyManager
    try:
        ct = fm.CurrentType
        if ct is None:
            t = Transaction(fdoc, u'Создать тип по умолчанию')
            t.Start()
            newt = fm.NewType(u'Тип 1')
            fm.CurrentType = newt
            t.Commit()
    except:
        pass

def _bool_to_int(b):
    return 1 if b else 0

def _read_back_value(fm, fp):
    try:
        ft = fm.CurrentType
        if ft:
            try:
                return ft.AsString(fp)
            except:
                pass
            try:
                return ft.AsInteger(fp)
            except:
                pass
            try:
                return ft.AsDouble(fp)
            except:
                pass
        return None
    except:
        return None

def _to_bool(text):
    s = (text or u'').strip().lower()
    return s in (u'1', u'true', u'истина', u'да', u'y', u'yes')

def _strip_quotes(s):
    if s is None: return s
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def _parse_number(text):
    try:
        return float((text or u'').strip().replace(',', '.'))
    except:
        return None

def _set_instance_default(fdoc, fm, fp, text_value):
    """Присвоить значение по умолчанию для параметра ЭКЗЕМПЛЯРА на текущем типе."""
    if text_value is None: return False, u'пусто'
    d = fp.Definition
    ptype = d.ParameterType
    txt = text_value

    try:
        units = fdoc.GetUnits()
    except:
        units = None

    try:
        if ptype == ParameterType.Text or ptype.ToString() == 'Text':
            try:
                fm.Set(fp, _strip_quotes(txt))
                return True, u''
            except Exception as e_set_text:
                try:
                    # Фолбэк: попробуем как формулу (в кавычках)
                    fm.SetFormula(fp, '"%s"' % _strip_quotes(txt))
                    return True, u''
                except Exception as e_set_text2:
                    raise e_set_text2

        if ptype == ParameterType.YesNo or ptype.ToString() == 'YesNo':
            fm.Set(fp, int(_bool_to_int(_to_bool(txt))))
            return True, u''

        if ptype == ParameterType.Integer or ptype.ToString() == 'Integer':
            num = _parse_number(txt)
            if num is None: return False, u'некорректное число'
            fm.Set(fp, int(round(num)))
            return True, u''

        if ptype == ParameterType.Number or ptype.ToString() == 'Number':
            num = _parse_number(txt)
            if num is None: return False, u'некорректное число'
            fm.Set(fp, float(num))
            return True, u''

        if ptype == ParameterType.Length or ptype.ToString() == 'Length':
            from Autodesk.Revit.DB import UnitType, UnitUtils
            num = _parse_number(txt)
            if num is None: return False, u'некорректное число'
            du = units.GetFormatOptions(UnitType.UT_Length).DisplayUnits if units else None
            val = UnitUtils.ConvertToInternalUnits(num, du) if du else num
            fm.Set(fp, float(val))
            return True, u''

        if ptype == ParameterType.Area or ptype.ToString() == 'Area':
            from Autodesk.Revit.DB import UnitType, UnitUtils
            num = _parse_number(txt)
            if num is None: return False, u'некорректное число'
            du = units.GetFormatOptions(UnitType.UT_Area).DisplayUnits if units else None
            val = UnitUtils.ConvertToInternalUnits(num, du) if du else num
            fm.Set(fp, float(val))
            return True, u''

        if ptype == ParameterType.Volume or ptype.ToString() == 'Volume':
            from Autodesk.Revit.DB import UnitType, UnitUtils
            num = _parse_number(txt)
            if num is None: return False, u'некорректное число'
            du = units.GetFormatOptions(UnitType.UT_Volume).DisplayUnits if units else None
            val = UnitUtils.ConvertToInternalUnits(num, du) if du else num
            fm.Set(fp, float(val))
            return True, u''

        if ptype == ParameterType.Angle or ptype.ToString() == 'Angle':
            from Autodesk.Revit.DB import UnitType, UnitUtils
            num = _parse_number(txt)
            if num is None: return False, u'некорректное число'
            du = units.GetFormatOptions(UnitType.UT_Angle).DisplayUnits if units else None
            val = UnitUtils.ConvertToInternalUnits(num, du) if du else num
            fm.Set(fp, float(val))
            return True, u''

        # Fallback — строкой
        fm.Set(fp, txt)
        return True, u''
    except Exception as e:
        return False, u'%s' % e

def add_param_to_familydoc(fdoc, extdef, bipg, is_instance, formula_text):
    if not extdef: return (False, u'Не найден ExtDef')
    fm = fdoc.FamilyManager
    if get_existing_family_param_by_name(fm, extdef.Name):
        return (False, u'Существует')
    _ensure_family_has_type(fdoc)

    # 1) Добавляем параметр отдельной транзакцией
    t1 = Transaction(fdoc, u'Добавить общий параметр: {}'.format(extdef.Name))
    try:
        t1.Start()
        fp = fm.AddParameter(extdef, bipg, is_instance)
        formula_applied = False
        formula_err = None
        if (not is_instance) and formula_text and formula_text.strip():
            try:
                ftxt = (formula_text or u'').strip().replace(',', '.')
                if ftxt:
                    fm.SetFormula(fp, ftxt)
                    formula_applied = True
            except Exception as ee:
                try:
                    fp.Formula = ftxt
                    formula_applied = True
                except Exception as ee2:
                    formula_err = u'{} / {}'.format(ee, ee2)
        t1.Commit()
    except Exception as e:
        try: t1.RollBack()
        except: pass
        return (False, u'Ошибка при добавлении: {}'.format(e))

    # 2) Для ЭКЗЕМПЛЯРА: проставим значение второй транзакцией
    value_applied = False
    value_err = None
    if is_instance and formula_text and formula_text.strip():
        try:
            fp2 = get_existing_family_param_by_name(fm, extdef.Name)
            t2 = Transaction(fdoc, u'Значение экземпляра: {}'.format(extdef.Name))
            t2.Start()
            ok, err = _set_instance_default(fdoc, fm, fp2, formula_text)
            if ok:
                value_applied = True
            else:
                value_err = err
            # диагностика
            try:
                rb = _read_back_value(fm, fp2)
                print(u'      [readback] {} = {}'.format(fp2.Definition.Name, rb))
            except:
                pass
            t2.Commit()
        except Exception as e2:
            try: t2.RollBack()
            except: pass
            value_err = u'%s' % e2

    if not is_instance:
        if formula_applied: return (True, u'Добавлен (формула применена)')
        elif formula_text and formula_text.strip(): return (True, u'Добавлен (без формулы: {})'.format(formula_err or u''))
        else: return (True, u'Добавлен')
    else:
        if value_applied: return (True, u'Добавлен (значение экземпляра установлено)')
        elif formula_text and formula_text.strip(): return (True, u'Добавлен (значение не применено: {})'.format(value_err or u''))
        else: return (True, u'Добавлен')

# --------------------------- Data Model -------------------------------------

class QueueItem(object):
    def __init__(self, name, guid, groupname, is_instance, bipg, formula=u''):
        self.Name = name
        self.Guid = guid
        self.GroupName = groupname
        self.IsInstance = bool(is_instance)
        self.Bipg = bipg
        self.Formula = formula or u''

    def to_json(self):
        return {
            'name': self.Name,
            'guid': str(self.Guid),
            'groupname': self.GroupName,
            'is_instance': bool(self.IsInstance),
            'bipg': self.Bipg.ToString() if self.Bipg else '',
            'formula': self.Formula or u''
        }

    @staticmethod
    def from_json(d):
        bipg = getattr(BuiltInParameterGroup, d.get('bipg',''), None)
        if bipg is None:
            bipg = BuiltInParameterGroup.PG_IDENTITY_DATA
        return QueueItem(
            d.get('name', u''),
            d.get('guid', u''),
            d.get('groupname', u''),
            bool(d.get('is_instance', True)),
            bipg,
            d.get('formula', u'')
        )

# ------------------------------ XAML UI -------------------------------------

XAML = u"""
<Window xmlns='http://schemas.microsoft.com/winfx/2006/xaml/presentation'
        xmlns:x='http://schemas.microsoft.com/winfx/2006/xaml'
        Title='Пакетное добавление общих параметров' Height='660' Width='900'
        WindowStartupLocation='CenterScreen' Background='#FFF'>
  <Grid Margin='10'>
    <Grid.ColumnDefinitions>
      <ColumnDefinition Width='260'/>
      <ColumnDefinition Width='10'/>
      <ColumnDefinition Width='*'/>
    </Grid.ColumnDefinitions>
    <Grid.RowDefinitions>
      <RowDefinition Height='Auto'/>
      <RowDefinition Height='*'/>
      <RowDefinition Height='Auto'/>
    </Grid.RowDefinitions>

    <GroupBox Header='Добавить параметры в:' Grid.Column='0' Grid.Row='0' Padding='8' Margin='0,0,0,8'>
      <StackPanel>
        <RadioButton x:Name='rbActive' Content='Активное семейство' IsChecked='True' Margin='0,0,0,6'/>
        <RadioButton x:Name='rbOpen' Content='Все открытые семейства' Margin='0,0,0,6'/>
        <RadioButton x:Name='rbFolder' Content='Семейства в выбранной папке' Margin='0,0,0,6'/>
        <StackPanel Orientation='Horizontal' Margin='0,4,0,0'>
          <Button x:Name='btnPickFolder' Content='Выберите папку с семействами' Width='220' IsEnabled='False'/>
        </StackPanel>
      </StackPanel>
    </GroupBox>

    <Border Grid.Column='1'/>

    <Grid Grid.Column='2'>
      <Grid.RowDefinitions>
        <RowDefinition Height='240'/>
        <RowDefinition Height='Auto'/>
        <RowDefinition Height='*'/>
      </Grid.RowDefinitions>

      <Grid Grid.Row='0'>
        <Grid.ColumnDefinitions>
          <ColumnDefinition Width='*'/>
          <ColumnDefinition Width='10'/>
          <ColumnDefinition Width='*'/>
        </Grid.ColumnDefinitions>
        <GroupBox Header='Группа общих параметров:' Grid.Column='0' Padding='6' Margin='0,0,0,8'>
          <ListBox x:Name='lbGroups'/>
        </GroupBox>
        <Border Grid.Column='1'/>
        <GroupBox Header='Общие параметры:' Grid.Column='2' Padding='6' Margin='0,0,0,8'>
          <ListBox x:Name='lbParams' SelectionMode='Extended'/>
        </GroupBox>
      </Grid>

      <Grid Grid.Row='1' Margin='0,0,0,8'>
        <Grid.ColumnDefinitions>
          <ColumnDefinition Width='*'/>
          <ColumnDefinition Width='*'/>
          <ColumnDefinition Width='*'/>
          <ColumnDefinition Width='Auto'/>
        </Grid.ColumnDefinitions>
        <StackPanel Orientation='Vertical' Grid.Column='0' Margin='0,0,8,0'>
          <TextBlock Text='Группирование параметров:' Margin='0,0,0,4'/>
          <ComboBox x:Name='cbBipg' MinWidth='180'/>
        </StackPanel>
        <StackPanel Grid.Column='1' Orientation='Vertical' Margin='0,0,8,0'>
          <TextBlock Text='Добавить в:' Margin='0,0,0,4'/>
          <StackPanel Orientation='Horizontal'>
            <RadioButton x:Name='rbType' Content='Тип' Margin='0,0,16,0'/>
            <RadioButton x:Name='rbInst' Content='Экземпляр' IsChecked='True'/>
          </StackPanel>
        </StackPanel>
        <StackPanel Grid.Column='2' Orientation='Vertical' Margin='0,0,8,0'>
          <TextBlock Text='Формула / значение (для экз.)' Margin='0,0,0,4'/>
          <TextBox x:Name='tbFormula'/>
        </StackPanel>
        <StackPanel Grid.Column='3' Orientation='Vertical' HorizontalAlignment='Right'>
          <Button x:Name='btnAdd' Content='Добавить ▶' Width='120' Height='30' Margin='0,18,0,0'/>
        </StackPanel>
      </Grid>

      <GroupBox Grid.Row='2' Header='Очередь добавления' Padding='6'>
        <DockPanel>
          <StackPanel DockPanel.Dock='Bottom' Orientation='Horizontal' HorizontalAlignment='Left' Margin='0,6,0,0'>
            <Button x:Name='btnOpen' Content='Открыть' Width='90' Margin='0,0,6,0'/>
            <Button x:Name='btnSave' Content='Сохранить' Width='110' Margin='0,0,6,0'/>
            <Button x:Name='btnRemove' Content='Удалить ▲' Width='110'/>
          </StackPanel>
          <DataGrid x:Name='dgQueue' AutoGenerateColumns='False' CanUserAddRows='False' IsReadOnly='False'>
            <DataGrid.Columns>
              <DataGridTextColumn Header='Параметр' Binding='{Binding Name}' IsReadOnly='True' Width='*'/>
              <DataGridCheckBoxColumn Header='Экземпляр' Binding='{Binding IsInstance, Mode=TwoWay}' Width='100'/>
              <DataGridComboBoxColumn x:Name='colBipg' Header='Группирование'
                                      SelectedValueBinding='{Binding Bipg, Mode=TwoWay}' Width='200'/>
              <DataGridTextColumn Header='Формула / Значение'
                                  Binding='{Binding Formula, Mode=TwoWay, UpdateSourceTrigger=PropertyChanged}'
                                  Width='260'/>
            </DataGrid.Columns>
          </DataGrid>
        </DockPanel>
      </GroupBox>
    </Grid>

    <StackPanel Grid.Row='2' Grid.ColumnSpan='3' Orientation='Horizontal' HorizontalAlignment='Right' Margin='0,10,0,0'>
      <Button x:Name='btnOk' Content='Ok' Width='100' Margin='0,0,8,0'/>
      <Button x:Name='btnCancel' Content='Отмена' Width='100'/>
    </StackPanel>

  </Grid>
</Window>
"""

# ----------------------------- Window Logic ---------------------------------

class MainController(object):
    def __init__(self):
        sr = StringReader(XAML)
        xr = XmlReader.Create(sr)
        self.w = XamlReader.Load(xr)

        # Bind controls
        self.rbActive = self.w.FindName('rbActive')
        self.rbOpen = self.w.FindName('rbOpen')
        self.rbFolder = self.w.FindName('rbFolder')
        self.btnPickFolder = self.w.FindName('btnPickFolder')
        self.lbGroups = self.w.FindName('lbGroups')
        self.lbParams = self.w.FindName('lbParams')
        self.cbBipg = self.w.FindName('cbBipg')
        self.rbType = self.w.FindName('rbType')
        self.rbInst = self.w.FindName('rbInst')
        self.tbFormula = self.w.FindName('tbFormula')
        self.btnAdd = self.w.FindName('btnAdd')
        self.dgQueue = self.w.FindName('dgQueue')
        self.colBipg = self.w.FindName('colBipg')
        self.btnOpen = self.w.FindName('btnOpen')
        self.btnSave = self.w.FindName('btnSave')
        self.btnRemove = self.w.FindName('btnRemove')
        self.btnOk = self.w.FindName('btnOk')
        self.btnCancel = self.w.FindName('btnCancel')

        # State
        self._queue = ObservableCollection[QueueItem]()
        self.dgQueue.ItemsSource = self._queue
        self._picked_folder = None

        # Events
        self.rbActive.Checked += self._on_mode_changed
        self.rbOpen.Checked += self._on_mode_changed
        self.rbFolder.Checked += self._on_mode_changed
        self._on_mode_changed()

        self.btnPickFolder.Click += self._pick_folder

        # Shared parameter file / groups
        self._dfile = ensure_shared_parameters_def_file()
        if not self._dfile:
            return
        self._group_by_name = {}
        self.lbGroups.Items.Clear()
        groups = list(self._dfile.Groups)
        for g in groups:
            self.lbGroups.Items.Add(g.Name)
            self._group_by_name[g.Name] = g
        if groups:
            self.lbGroups.SelectedIndex = 0
        self.lbGroups.SelectionChanged += self._on_group_changed
        self._on_group_changed()

        # BIPG options
        self._bipg_items = all_bipg_options()
        self.cbBipg.Items.Clear()
        default_index = 0
        for i,(label, enumv) in enumerate(self._bipg_items):
            self.cbBipg.Items.Add(label)
            if label.lower().strip() in (u'прочее', u'общие'):
                default_index = i
        self.cbBipg.SelectedIndex = default_index

        # Build ItemsSource for row ComboBox column (label->enum)
        kv = List[KeyValuePair[System.String, BuiltInParameterGroup]]()
        for (label, enumv) in self._bipg_items:
            kv.Add(KeyValuePair[System.String, BuiltInParameterGroup](label, enumv))
        self._bipg_kv = kv
        self.colBipg.ItemsSource = self._bipg_kv
        self.colBipg.DisplayMemberPath = 'Key'
        self.colBipg.SelectedValuePath = 'Value'

        # Buttons
        self.btnAdd.Click += self._add_to_queue
        self.btnRemove.Click += self._remove_selected
        self.btnSave.Click += self._save_queue
        self.btnOpen.Click += self._open_queue
        self.btnCancel.Click += self._on_cancel
        self.btnOk.Click += self._run

    # --- UI helpers ---
    def _on_mode_changed(self, sender=None, args=None):
        self.btnPickFolder.IsEnabled = self.rbFolder.IsChecked

    def _pick_folder(self, sender, args):
        fbd = FolderBrowserDialog()
        res = fbd.ShowDialog()
        if res == DialogResult.OK:
            self._picked_folder = fbd.SelectedPath
            self.btnPickFolder.Content = fbd.SelectedPath

    def _on_group_changed(self, sender=None, args=None):
        self.lbParams.Items.Clear()
        idx = self.lbGroups.SelectedIndex
        if idx < 0: return
        gname = self.lbGroups.SelectedItem
        g = self._group_by_name.get(gname)
        if not g: return
        for d in g.Definitions:
            try:
                if isinstance(d, ExternalDefinition):
                    self.lbParams.Items.Add(d.Name)
            except: pass

    def _add_to_queue(self, sender, args):
        if self.lbParams.SelectedItems is None or self.lbParams.SelectedItems.Count == 0:
            forms.alert(u'Выберите хотя бы один параметр справа.')
            return
        gname = self.lbGroups.SelectedItem
        g = self._group_by_name.get(gname)
        if not g:
            forms.alert(u'Группа общих параметров не выбрана.')
            return

        bipg = self._bipg_items[self.cbBipg.SelectedIndex][1] if self.cbBipg.SelectedIndex >= 0 else BuiltInParameterGroup.PG_IDENTITY_DATA
        is_inst = bool(self.rbInst.IsChecked)
        formula_text = self.tbFormula.Text or u''

        for pname in list(self.lbParams.SelectedItems):
            extdef = None
            for d in g.Definitions:
                if isinstance(d, ExternalDefinition) and d.Name == pname:
                    extdef = d; break
            if extdef is None: continue
            qi = QueueItem(extdef.Name, str(extdef.GUID), gname, is_inst, bipg, formula_text)
            if not any(x.Name == qi.Name for x in list(self._queue)):
                self._queue.Add(qi)

    def _remove_selected(self, sender, args):
        sel = list(self.dgQueue.SelectedItems) if self.dgQueue.SelectedItems else []
        if not sel: return
        for item in sel:
            self._queue.Remove(item)

    def _save_queue(self, sender, args):
        items = [qi.to_json() for qi in list(self._queue)]
        if not items:
            forms.alert(u'Очередь пуста — сохранять нечего.')
            return
        ofd = OpenFileDialog()
        ofd.Title = u'Сохранить набор параметров (JSON)'
        ofd.Filter = 'JSON (*.json)|*.json'
        ofd.FileName = 'paramset.json'
        if ofd.ShowDialog() == DialogResult.OK:
            try:
                with open(ofd.FileName, 'wb') as fp:
                    fp.write(json.dumps(items, indent=2, ensure_ascii=False).encode('utf-8'))
            except Exception as e:
                forms.alert(u'Не удалось сохранить файл:\n{}'.format(e))

    def _open_queue(self, sender, args):
        ofd = OpenFileDialog()
        ofd.Title = u'Открыть набор параметров (JSON)'
        ofd.Filter = 'JSON (*.json)|*.json|All files (*.*)|*.*'
        if ofd.ShowDialog() == DialogResult.OK:
            try:
                raw = open(ofd.FileName,'rb').read()
                try:
                    data = json.loads(raw)
                except:
                    data = json.loads(raw.decode('utf-8'))
                self._queue.Clear()
                for d in data:
                    qi = QueueItem.from_json(d)
                    self._queue.Add(qi)
            except Exception as e:
                forms.alert(u'Не удалось открыть файл:\n{}'.format(e))

    def _on_cancel(self, s, a):
        self.w.Close()

    # --- Run ---
    def _collect_target_family_docs(self):
        docs = []
        if self.rbActive.IsChecked:
            d = uidoc.Document
            if not d.IsFamilyDocument:
                forms.alert(u'Текущий документ — не семейство. Откройте семейство в редакторе и повторите.')
                return []
            docs.append(d)
            return docs

        if self.rbOpen.IsChecked:
            for d in __revit__.Application.Documents:
                try:
                    if d.IsFamilyDocument:
                        docs.append(d)
                except: pass
            if not docs:
                forms.alert(u'Открытых семейств не найдено.')
            return docs

        if self.rbFolder.IsChecked:
            if not self._picked_folder or not os.path.isdir(self._picked_folder):
                forms.alert(u'Не выбрана папка с семействами.')
                return []
            file_paths = list(walk_rfa_files(self._picked_folder, recursive=True))
            if not file_paths:
                forms.alert(u'В папке нет файлов *.rfa')
                return []
            for p in file_paths:
                try:
                    odoc = app.OpenDocumentFile(p)
                    if odoc and odoc.IsFamilyDocument:
                        docs.append(odoc)
                except Exception as e:
                    logger.error('Не удалось открыть {}: {}'.format(p, e))
            return docs
        return []

    def _resolve_extdef_by_guid(self, guid_str):
        try:
            g = Guid(guid_str)
        except: return None
        for grp in self._dfile.Groups:
            for d in grp.Definitions:
                try:
                    if isinstance(d, ExternalDefinition) and str(d.GUID) == str(g):
                        return d
                except: pass
        return None

    def _run(self, sender, args):
        # фиксация последних правок в таблице
        try:
            from System.Windows.Controls import DataGridEditingUnit
            self.dgQueue.CommitEdit(DataGridEditingUnit.Cell, True)
            self.dgQueue.CommitEdit(DataGridEditingUnit.Row, True)
        except: pass

        items = list(self._queue)
        if not items:
            forms.alert(u'Очередь пуста. Добавьте параметры.')
            return
        docs = self._collect_target_family_docs()
        if not docs:
            return

        total_added = 0
        total_skipped = 0
        total_errors = 0
        try: output.activate()
        except: pass
        print(u'Начинаем добавление параметров...')
        for d in docs:
            print(u'\nСемейство: {}'.format(d.Title))
            fm = d.FamilyManager
            for qi in items:
                print(u'  → {} | Экз:{} | Группа:{} | Формула/Значение:"{}"'.format(qi.Name, qi.IsInstance, qi.Bipg, qi.Formula))
                extdef = self._resolve_extdef_by_guid(qi.Guid)
                if extdef is None:
                    grp = None
                    for g in self._dfile.Groups:
                        if g.Name == qi.GroupName:
                            grp = g; break
                    if grp:
                        for dd in grp.Definitions:
                            if isinstance(dd, ExternalDefinition) and dd.Name == qi.Name:
                                extdef = dd; break
                if extdef is None:
                    print(u'    - не найден в файле общих параметров')
                    total_errors += 1
                    continue
                added, msg = add_param_to_familydoc(d, extdef, qi.Bipg, qi.IsInstance, qi.Formula)
                print(u'    - {}'.format(msg))
                if added: total_added += 1
                elif msg.startswith(u'Существует'): total_skipped += 1
                else: total_errors += 1
            if self.rbFolder.IsChecked:
                try:
                    d.Save()
                    d.Close(False)
                except Exception as e:
                    logger.error('Ошибка сохранения/закрытия {}: {}'.format(d.Title, e))

        forms.alert(u'Готово!\nДобавлено: {}\nПропущено (уже были): {}\nОшибок: {}'.format(total_added, total_skipped, total_errors))
        self.w.Close()

# ------------------------------ Run UI --------------------------------------
try:
    ctrl = MainController()
    ctrl.w.ShowDialog()
except Exception as e:
    tb = traceback.format_exc()
    forms.alert(u'Ошибка запуска окна:\n{}\n\n{}'.format(e, tb))
