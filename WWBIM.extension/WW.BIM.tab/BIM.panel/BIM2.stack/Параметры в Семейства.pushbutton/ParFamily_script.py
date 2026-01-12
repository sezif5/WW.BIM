
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
from System.Windows.Forms import FolderBrowserDialog, OpenFileDialog, SaveFileDialog, DialogResult
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

def safe_rollback(transaction, context=''):
    """Безопасный откат транзакции с логированием."""
    if transaction is None:
        return
    try:
        if transaction.HasStarted() and not transaction.HasEnded():
            transaction.RollBack()
    except Exception as e:
        logger.debug(u'Rollback failed{}: {}'.format(
            u' (' + context + u')' if context else u'', e))

def safe_call(func, default=None, context='', log_level='debug'):
    """Безопасный вызов функции с логированием ошибок."""
    try:
        return func()
    except Exception as e:
        msg = u'{}: {}'.format(context, e) if context else unicode(e)
        if log_level == 'warning':
            logger.warning(msg)
        elif log_level == 'error':
            logger.error(msg)
        else:
            logger.debug(msg)
        return default

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
    """Получить список всех групп параметров с их локализованными названиями."""
    items = []
    for name in dir(BuiltInParameterGroup):
        if name.startswith('PG_') or name == 'INVALID':
            try:
                enum_val = getattr(BuiltInParameterGroup, name)
                label = LabelUtils.GetLabelFor(enum_val)
                if label and label.strip():
                    items.append((label, enum_val))
            except Exception as e:
                logger.debug(u'Пропущена группа параметров {}: {}'.format(name, e))
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
    """Найти параметр семейства по имени."""
    for fp in fm.Parameters:
        try:
            if fp.Definition and fp.Definition.Name == name:
                return fp
        except Exception as e:
            logger.debug(u'Ошибка при проверке параметра: {}'.format(e))
    return None

def _ensure_family_has_type(fdoc):
    """Убедиться, что в семействе есть хотя бы один тип."""
    fm = fdoc.FamilyManager
    t = None
    try:
        ct = fm.CurrentType
        if ct is None:
            t = Transaction(fdoc, u'Создать тип по умолчанию')
            t.Start()
            newt = fm.NewType(u'Тип 1')
            fm.CurrentType = newt
            t.Commit()
    except Exception as e:
        safe_rollback(t, u'создание типа')
        logger.warning(u'Не удалось создать тип по умолчанию: {}'.format(e))

def _bool_to_int(b):
    return 1 if b else 0

def _read_back_value(fm, fp):
    """Прочитать значение параметра (пробует разные типы)."""
    if fp is None:
        return None
    try:
        ft = fm.CurrentType
        if not ft:
            return u'(нет текущего типа)'

        storage_type = _get_storage_type(fp)
        st_str = str(storage_type) if storage_type else ''

        # Читаем в зависимости от типа хранения
        if 'String' in st_str:
            return ft.AsString(fp)
        elif 'Integer' in st_str:
            return ft.AsInteger(fp)
        elif 'Double' in st_str:
            return ft.AsDouble(fp)
        else:
            # Пробуем все методы
            for method in (ft.AsString, ft.AsInteger, ft.AsDouble):
                try:
                    result = method(fp)
                    if result is not None:
                        return result
                except Exception:
                    continue
        return None
    except Exception as e:
        return u'(ошибка: {})'.format(e)

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
    """Преобразовать текст в число (поддерживает запятую как разделитель)."""
    try:
        return float((text or u'').strip().replace(',', '.'))
    except ValueError:
        return None

def _get_storage_type(fp):
    """Получить тип хранения параметра (работает для всех версий Revit)."""
    try:
        return fp.StorageType
    except Exception:
        try:
            return fp.Definition.ParameterType
        except Exception:
            return None

def _get_param_type_name(fp):
    """Получить имя типа параметра для диагностики."""
    try:
        # Revit 2022+
        if hasattr(fp.Definition, 'GetDataType'):
            return str(fp.Definition.GetDataType())
        # Revit < 2022
        return str(fp.Definition.ParameterType)
    except Exception:
        return u'Unknown'

def _set_formula_or_value(fdoc, fm, fp, text_value):
    """Установить формулу или значение для параметра (экземпляра или типа)."""
    if text_value is None:
        return False, u'пусто'
    if fp is None:
        return False, u'параметр не найден'

    txt = text_value.strip()
    if not txt:
        return False, u'пустое значение'

    # Диагностика
    storage_type = _get_storage_type(fp)
    param_type_name = _get_param_type_name(fp)
    print(u'      [debug] StorageType={}, ParamType={}, Input="{}"'.format(
        storage_type, param_type_name, txt))

    # Подготовка формулы
    formula = txt
    # Для числовых формул заменяем запятую на точку (если это не строка в кавычках)
    if not formula.startswith('"'):
        formula = formula.replace(',', '.')

    # 1) Сначала пробуем установить как ФОРМУЛУ
    try:
        fm.SetFormula(fp, formula)
        print(u'      [formula] OK: "{}"'.format(formula))
        return True, u'формула'
    except Exception as e_formula:
        print(u'      [formula] Не удалось: {}'.format(e_formula))

    # 2) Если формула не сработала, пробуем как ЗНАЧЕНИЕ
    try:
        st_str = str(storage_type) if storage_type else ''

        if 'String' in st_str:
            fm.Set(fp, _strip_quotes(txt))
            print(u'      [value] OK (String)')
            return True, u'значение'

        elif 'Integer' in st_str:
            ptype_str = param_type_name.lower()
            if 'yesno' in ptype_str or 'boolean' in ptype_str:
                val = 1 if _to_bool(txt) else 0
            else:
                num = _parse_number(txt)
                if num is None:
                    return False, u'некорректное число'
                val = int(round(num))
            fm.Set(fp, val)
            print(u'      [value] OK (Integer): {}'.format(val))
            return True, u'значение'

        elif 'Double' in st_str:
            num = _parse_number(txt)
            if num is None:
                return False, u'некорректное число'
            fm.Set(fp, float(num))
            print(u'      [value] OK (Double): {}'.format(num))
            return True, u'значение'

        else:
            # Fallback
            fm.Set(fp, txt)
            print(u'      [value] OK (fallback)')
            return True, u'значение'

    except Exception as e_value:
        return False, u'формула: {} / значение: {}'.format(e_formula, e_value)

def add_param_to_familydoc(fdoc, extdef, bipg, is_instance, formula_text):
    """Добавить общий параметр в документ семейства."""
    if not extdef:
        return (False, u'Не найден ExtDef')

    fm = fdoc.FamilyManager
    param_name = extdef.Name

    # Проверяем, существует ли уже
    existing = get_existing_family_param_by_name(fm, param_name)
    if existing:
        return (False, u'Существует')

    _ensure_family_has_type(fdoc)

    # 1) Добавляем параметр (без формулы - она будет применена отдельной транзакцией)
    t1 = Transaction(fdoc, u'Добавить параметр: {}'.format(param_name))
    try:
        t1.Start()
        fm.AddParameter(extdef, bipg, is_instance)
        t1.Commit()
    except Exception as e:
        safe_rollback(t1, param_name)
        return (False, u'Ошибка добавления: {}'.format(e))

    # 2) Устанавливаем формулу/значение отдельной транзакцией
    formula_applied = False
    formula_err = None

    if formula_text and formula_text.strip():
        # Получаем параметр заново после коммита
        fp = get_existing_family_param_by_name(fm, param_name)
        if fp is None:
            formula_err = u'параметр не найден после добавления'
            print(u'      [error] {}'.format(formula_err))
        else:
            t2 = None
            try:
                t2 = Transaction(fdoc, u'Формула: {}'.format(param_name))
                t2.Start()

                ok, result_type = _set_formula_or_value(fdoc, fm, fp, formula_text)
                if ok:
                    formula_applied = True
                    formula_err = result_type  # 'формула' или 'значение'
                else:
                    formula_err = result_type

                # Проверка: читаем значение обратно
                rb = _read_back_value(fm, fp)
                print(u'      [readback] {} = {}'.format(param_name, rb))

                t2.Commit()
            except Exception as e2:
                safe_rollback(t2, param_name)
                formula_err = unicode(e2)
                print(u'      [error] Исключение: {}'.format(e2))

    # Формируем результат
    if formula_applied:
        return (True, u'Добавлен + {}'.format(formula_err))
    elif formula_text and formula_text.strip():
        return (True, u'Добавлен (не применено: {})'.format(formula_err or u'?'))
    else:
        return (True, u'Добавлен')

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

SCRIPT_DIR = os.path.dirname(__file__)
XAML_FILE = os.path.join(SCRIPT_DIR, 'MainWindow.xaml')

def load_xaml_window(xaml_path):
    """Загрузить WPF окно из XAML файла."""
    with open(xaml_path, 'rb') as f:
        xaml_content = f.read().decode('utf-8')
    sr = StringReader(xaml_content)
    xr = XmlReader.Create(sr)
    return XamlReader.Load(xr)

# ----------------------------- Window Logic ---------------------------------

class MainController(object):
    def __init__(self):
        self.w = load_xaml_window(XAML_FILE)

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
        if idx < 0:
            return
        gname = self.lbGroups.SelectedItem
        g = self._group_by_name.get(gname)
        if not g:
            return
        for d in g.Definitions:
            try:
                if isinstance(d, ExternalDefinition):
                    self.lbParams.Items.Add(d.Name)
            except Exception as e:
                logger.debug(u'Ошибка при чтении определения параметра: {}'.format(e))

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
        sfd = SaveFileDialog()
        sfd.Title = u'Сохранить набор параметров (JSON)'
        sfd.Filter = 'JSON (*.json)|*.json'
        sfd.FileName = 'paramset.json'
        if sfd.ShowDialog() == DialogResult.OK:
            try:
                with open(sfd.FileName, 'wb') as fp:
                    fp.write(json.dumps(items, indent=2, ensure_ascii=False).encode('utf-8'))
            except Exception as e:
                forms.alert(u'Не удалось сохранить файл:\n{}'.format(e))

    def _open_queue(self, sender, args):
        ofd = OpenFileDialog()
        ofd.Title = u'Открыть набор параметров (JSON)'
        ofd.Filter = 'JSON (*.json)|*.json|All files (*.*)|*.*'
        if ofd.ShowDialog() == DialogResult.OK:
            try:
                raw = open(ofd.FileName, 'rb').read()
                try:
                    data = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    data = json.loads(raw.decode('utf-8'))
                self._queue.Clear()
                for d in data:
                    qi = QueueItem.from_json(d)
                    self._queue.Add(qi)
            except Exception as e:
                logger.error(u'Ошибка загрузки файла {}: {}'.format(ofd.FileName, e))
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
                except Exception as e:
                    logger.debug(u'Пропущен документ: {}'.format(e))
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
        """Найти определение параметра по GUID."""
        try:
            g = Guid(guid_str)
        except Exception:
            logger.debug(u'Некорректный GUID: {}'.format(guid_str))
            return None
        for grp in self._dfile.Groups:
            for d in grp.Definitions:
                try:
                    if isinstance(d, ExternalDefinition) and str(d.GUID) == str(g):
                        return d
                except Exception as e:
                    logger.debug(u'Ошибка при сравнении GUID: {}'.format(e))
        return None

    def _run(self, sender, args):
        # фиксация последних правок в таблице
        try:
            from System.Windows.Controls import DataGridEditingUnit
            self.dgQueue.CommitEdit(DataGridEditingUnit.Cell, True)
            self.dgQueue.CommitEdit(DataGridEditingUnit.Row, True)
        except Exception as e:
            logger.debug(u'Не удалось зафиксировать правки таблицы: {}'.format(e))

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
        try:
            output.activate()
        except Exception:
            pass  # Окно вывода может быть недоступно
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
