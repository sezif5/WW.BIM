# -*- coding: utf-8 -*-
# pyRevit button script: Add Shared Parameters to Project
# IronPython 2.7 (pyRevit), Revit 2020–2025

from __future__ import print_function, division

import sys
import os
import json
import traceback
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
from System.Windows.Forms import OpenFileDialog, SaveFileDialog, DialogResult
from System.Collections.Generic import List, KeyValuePair
from System.ComponentModel import INotifyPropertyChanged, PropertyChangedEventArgs

# Revit API
clr.AddReference('RevitAPI')
clr.AddReference('RevitAPIUI')
from Autodesk.Revit.DB import (
    Transaction, BuiltInParameterGroup, LabelUtils,
    ExternalDefinition, InstanceBinding, TypeBinding,
    Category, CategorySet, CategoryType, BuiltInCategory
)
from Autodesk.Revit.UI import TaskDialog

# pyRevit
from pyrevit import forms, script

logger = script.get_logger()
output = script.get_output()
uidoc = __revit__.ActiveUIDocument
doc = uidoc.Document
app = __revit__.Application

# ========================== CATEGORY PRESETS ==========================

# Все моделируемые категории (архитектура + конструкции + инженерия)
PRESET_ALL_MODEL = [
    # Общестроительные
    BuiltInCategory.OST_Walls,
    BuiltInCategory.OST_Floors,
    BuiltInCategory.OST_Roofs,
    BuiltInCategory.OST_Ceilings,
    BuiltInCategory.OST_Doors,
    BuiltInCategory.OST_Windows,
    BuiltInCategory.OST_Stairs,
    BuiltInCategory.OST_StairsRailing,
    BuiltInCategory.OST_Ramps,
    BuiltInCategory.OST_Columns,
    BuiltInCategory.OST_StructuralColumns,
    BuiltInCategory.OST_StructuralFraming,
    BuiltInCategory.OST_StructuralFoundation,
    BuiltInCategory.OST_Rebar,
    BuiltInCategory.OST_GenericModel,
    BuiltInCategory.OST_Furniture,
    BuiltInCategory.OST_FurnitureSystems,
    BuiltInCategory.OST_Casework,
    BuiltInCategory.OST_Parking,
    BuiltInCategory.OST_Planting,
    BuiltInCategory.OST_Site,
    BuiltInCategory.OST_Topography,
    BuiltInCategory.OST_SpecialityEquipment,
    BuiltInCategory.OST_Mass,
    BuiltInCategory.OST_Curtain_Systems,
    BuiltInCategory.OST_CurtainWallPanels,
    BuiltInCategory.OST_CurtainWallMullions,
    # Инженерные (MEP)
    BuiltInCategory.OST_MechanicalEquipment,
    BuiltInCategory.OST_DuctCurves,
    BuiltInCategory.OST_DuctFitting,
    BuiltInCategory.OST_DuctAccessory,
    BuiltInCategory.OST_DuctTerminal,
    BuiltInCategory.OST_FlexDuctCurves,
    BuiltInCategory.OST_DuctInsulations,
    BuiltInCategory.OST_DuctLinings,
    BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_PipeInsulations,
    BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_Sprinklers,
    BuiltInCategory.OST_ElectricalEquipment,
    BuiltInCategory.OST_ElectricalFixtures,
    BuiltInCategory.OST_LightingFixtures,
    BuiltInCategory.OST_LightingDevices,
    BuiltInCategory.OST_DataDevices,
    BuiltInCategory.OST_CommunicationDevices,
    BuiltInCategory.OST_FireAlarmDevices,
    BuiltInCategory.OST_SecurityDevices,
    BuiltInCategory.OST_NurseCallDevices,
    BuiltInCategory.OST_TelephoneDevices,
    BuiltInCategory.OST_CableTray,
    BuiltInCategory.OST_CableTrayFitting,
    BuiltInCategory.OST_Conduit,
    BuiltInCategory.OST_ConduitFitting,
    BuiltInCategory.OST_Wire,
]

PRESET_ARCHITECTURE = [
    BuiltInCategory.OST_Walls,
    BuiltInCategory.OST_Floors,
    BuiltInCategory.OST_Roofs,
    BuiltInCategory.OST_Ceilings,
    BuiltInCategory.OST_Doors,
    BuiltInCategory.OST_Windows,
    BuiltInCategory.OST_Stairs,
    BuiltInCategory.OST_StairsRailing,
    BuiltInCategory.OST_Ramps,
    BuiltInCategory.OST_Columns,
    BuiltInCategory.OST_GenericModel,
    BuiltInCategory.OST_Furniture,
    BuiltInCategory.OST_FurnitureSystems,
    BuiltInCategory.OST_Casework,
    BuiltInCategory.OST_SpecialityEquipment,
    BuiltInCategory.OST_Curtain_Systems,
    BuiltInCategory.OST_CurtainWallPanels,
    BuiltInCategory.OST_CurtainWallMullions,
]

PRESET_STRUCTURAL = [
    BuiltInCategory.OST_StructuralColumns,
    BuiltInCategory.OST_StructuralFraming,
    BuiltInCategory.OST_StructuralFoundation,
    BuiltInCategory.OST_Rebar,
    BuiltInCategory.OST_Floors,
    BuiltInCategory.OST_Walls,
]

PRESET_MEP = [
    BuiltInCategory.OST_MechanicalEquipment,
    BuiltInCategory.OST_DuctCurves,
    BuiltInCategory.OST_DuctFitting,
    BuiltInCategory.OST_DuctAccessory,
    BuiltInCategory.OST_DuctTerminal,
    BuiltInCategory.OST_FlexDuctCurves,
    BuiltInCategory.OST_DuctInsulations,
    BuiltInCategory.OST_DuctLinings,
    BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_PipeInsulations,
    BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_Sprinklers,
    BuiltInCategory.OST_ElectricalEquipment,
    BuiltInCategory.OST_ElectricalFixtures,
    BuiltInCategory.OST_LightingFixtures,
    BuiltInCategory.OST_LightingDevices,
    BuiltInCategory.OST_DataDevices,
    BuiltInCategory.OST_CommunicationDevices,
    BuiltInCategory.OST_FireAlarmDevices,
    BuiltInCategory.OST_SecurityDevices,
    BuiltInCategory.OST_CableTray,
    BuiltInCategory.OST_CableTrayFitting,
    BuiltInCategory.OST_Conduit,
    BuiltInCategory.OST_ConduitFitting,
]

# ========================== UTILITIES ==========================

def safe_rollback(transaction, context=''):
    """Безопасный откат транзакции."""
    if transaction is None:
        return
    try:
        if transaction.HasStarted() and not transaction.HasEnded():
            transaction.RollBack()
    except Exception as e:
        logger.debug(u'Rollback failed{}: {}'.format(
            u' (' + context + u')' if context else u'', e))


def ensure_shared_parameters_file():
    """Убедиться что файл общих параметров подключен."""
    sp_path = app.SharedParametersFilename
    if not sp_path or not os.path.exists(sp_path):
        ofd = OpenFileDialog()
        ofd.Filter = 'Shared Parameter file (*.txt)|*.txt|All files (*.*)|*.*'
        ofd.Title = u'Выберите файл общих параметров (.txt)'
        if ofd.ShowDialog() == DialogResult.OK:
            try:
                app.SharedParametersFilename = ofd.FileName
            except Exception as e:
                forms.alert(u'Не удалось назначить файл:\n{}'.format(e), exitscript=True)
        else:
            return None
    dfile = app.OpenSharedParameterFile()
    if dfile is None:
        forms.alert(u'Не удалось открыть файл общих параметров', exitscript=True)
    return dfile


def get_all_bipg_options():
    """Получить все группы параметров."""
    items = []
    for name in dir(BuiltInParameterGroup):
        if name.startswith('PG_') or name == 'INVALID':
            try:
                enum_val = getattr(BuiltInParameterGroup, name)
                label = LabelUtils.GetLabelFor(enum_val)
                if label and label.strip():
                    items.append((label, enum_val))
            except Exception:
                pass
    items.sort(key=lambda t: t[0].lower())
    return items


def get_model_categories():
    """Получить все категории модели, к которым можно привязать параметры."""
    categories = []
    for cat in doc.Settings.Categories:
        try:
            if cat.CategoryType == CategoryType.Model and cat.AllowsBoundParameters:
                categories.append(cat)
        except Exception:
            pass
    categories.sort(key=lambda c: c.Name)
    return categories


def is_param_already_bound(param_name):
    """Проверить, привязан ли уже параметр к проекту."""
    bindings = doc.ParameterBindings
    it = bindings.ForwardIterator()
    while it.MoveNext():
        try:
            if it.Key.Name == param_name:
                return True
        except Exception:
            pass
    return False


def add_param_to_project(extdef, bipg, is_instance, category_set):
    """Добавить общий параметр в проект."""
    if not extdef:
        return False, u'Не найден ExternalDefinition'

    if is_param_already_bound(extdef.Name):
        return False, u'Уже существует'

    if category_set is None or category_set.Size == 0:
        return False, u'Не выбраны категории'

    print(u'      [debug] Категорий в наборе: {}'.format(category_set.Size))

    t = Transaction(doc, u'Добавить параметр: {}'.format(extdef.Name))
    try:
        t.Start()

        # Создаём привязку
        if is_instance:
            binding = InstanceBinding(category_set)
            print(u'      [debug] Создан InstanceBinding')
        else:
            binding = TypeBinding(category_set)
            print(u'      [debug] Создан TypeBinding')

        # Добавляем в документ
        result = doc.ParameterBindings.Insert(extdef, binding, bipg)
        print(u'      [debug] Insert result: {}'.format(result))

        if result:
            t.Commit()
            return True, u'Добавлен ({} категорий)'.format(category_set.Size)
        else:
            # Пробуем альтернативный метод - ReInsert
            try:
                result2 = doc.ParameterBindings.ReInsert(extdef, binding, bipg)
                print(u'      [debug] ReInsert result: {}'.format(result2))
                if result2:
                    t.Commit()
                    return True, u'Добавлен через ReInsert'
            except Exception as e2:
                print(u'      [debug] ReInsert failed: {}'.format(e2))

            t.RollBack()
            return False, u'Insert вернул False (параметр возможно уже существует)'

    except Exception as e:
        safe_rollback(t, extdef.Name)
        return False, u'{}'.format(e)


# ========================== DATA MODEL ==========================

class CategoryItem(object):
    """Элемент категории для списка."""
    def __init__(self, category):
        self._category = category
        self._is_selected = False

    @property
    def Category(self):
        return self._category

    @property
    def Name(self):
        return self._category.Name

    @property
    def Id(self):
        return self._category.Id.IntegerValue

    @property
    def IsSelected(self):
        return self._is_selected

    @IsSelected.setter
    def IsSelected(self, value):
        self._is_selected = value


class QueueItem(object):
    """Элемент очереди добавления."""
    def __init__(self, name, guid, groupname, is_instance, bipg, bipg_label, category_ids, category_names):
        self.Name = name
        self.Guid = guid
        self.GroupName = groupname
        self.IsInstance = bool(is_instance)
        self.Bipg = bipg
        self.BipgLabel = bipg_label
        self.CategoryIds = category_ids  # list of int
        self.CategoryNames = category_names  # list of str

    @property
    def BindingTypeText(self):
        return u'Экз.' if self.IsInstance else u'Тип'

    @property
    def BipgText(self):
        return self.BipgLabel or u''

    @property
    def CategoriesText(self):
        if len(self.CategoryNames) > 3:
            return u'{} (+{})'.format(u', '.join(self.CategoryNames[:3]), len(self.CategoryNames) - 3)
        return u', '.join(self.CategoryNames)

    def to_json(self):
        return {
            'name': self.Name,
            'guid': str(self.Guid),
            'groupname': self.GroupName,
            'is_instance': self.IsInstance,
            'bipg': self.Bipg.ToString() if self.Bipg else '',
            'bipg_label': self.BipgLabel,
            'category_ids': self.CategoryIds,
            'category_names': self.CategoryNames,
        }

    @staticmethod
    def from_json(d):
        bipg = getattr(BuiltInParameterGroup, d.get('bipg', ''), None)
        if bipg is None:
            bipg = BuiltInParameterGroup.PG_IDENTITY_DATA
        return QueueItem(
            d.get('name', u''),
            d.get('guid', u''),
            d.get('groupname', u''),
            d.get('is_instance', True),
            bipg,
            d.get('bipg_label', u''),
            d.get('category_ids', []),
            d.get('category_names', []),
        )


# ========================== XAML UI ==========================

SCRIPT_DIR = os.path.dirname(__file__)
XAML_FILE = os.path.join(SCRIPT_DIR, 'MainWindow.xaml')


def load_xaml_window(xaml_path):
    """Загрузить окно из XAML."""
    with open(xaml_path, 'rb') as f:
        xaml_content = f.read().decode('utf-8')
    sr = StringReader(xaml_content)
    xr = XmlReader.Create(sr)
    return XamlReader.Load(xr)


# ========================== CONTROLLER ==========================

class MainController(object):
    def __init__(self):
        self.w = load_xaml_window(XAML_FILE)

        # Bind controls
        self.lbGroups = self.w.FindName('lbGroups')
        self.lbParams = self.w.FindName('lbParams')
        self.cbBipg = self.w.FindName('cbBipg')
        self.rbType = self.w.FindName('rbType')
        self.rbInst = self.w.FindName('rbInst')
        self.btnAdd = self.w.FindName('btnAdd')
        self.lbCategories = self.w.FindName('lbCategories')
        self.tbCategoryFilter = self.w.FindName('tbCategoryFilter')
        self.tbSelectedCount = self.w.FindName('tbSelectedCount')
        self.btnPresetAll = self.w.FindName('btnPresetAll')
        self.btnPresetArch = self.w.FindName('btnPresetArch')
        self.btnPresetMep = self.w.FindName('btnPresetMep')
        self.btnPresetStruct = self.w.FindName('btnPresetStruct')
        self.btnClearCats = self.w.FindName('btnClearCats')
        self.dgQueue = self.w.FindName('dgQueue')
        self.btnOpenQueue = self.w.FindName('btnOpenQueue')
        self.btnSaveQueue = self.w.FindName('btnSaveQueue')
        self.btnRemove = self.w.FindName('btnRemove')
        self.btnOk = self.w.FindName('btnOk')
        self.btnCancel = self.w.FindName('btnCancel')

        # State
        self._queue = ObservableCollection[QueueItem]()
        self.dgQueue.ItemsSource = self._queue
        self._category_items = []
        self._all_category_items = []

        # Load shared parameters file
        self._dfile = ensure_shared_parameters_file()
        if not self._dfile:
            return

        # Fill groups
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

        # Fill BIPG
        self._bipg_items = get_all_bipg_options()
        self.cbBipg.Items.Clear()
        default_idx = 0
        for i, (label, enumv) in enumerate(self._bipg_items):
            self.cbBipg.Items.Add(label)
            if label.lower().strip() in (u'данные', u'прочее', u'общие'):
                default_idx = i
        self.cbBipg.SelectedIndex = default_idx

        # Fill categories
        self._load_categories()

        # Events
        self.btnAdd.Click += self._add_to_queue
        self.btnRemove.Click += self._remove_selected
        self.btnPresetAll.Click += lambda s, e: self._apply_preset(PRESET_ALL_MODEL)
        self.btnPresetArch.Click += lambda s, e: self._apply_preset(PRESET_ARCHITECTURE)
        self.btnPresetMep.Click += lambda s, e: self._apply_preset(PRESET_MEP)
        self.btnPresetStruct.Click += lambda s, e: self._apply_preset(PRESET_STRUCTURAL)
        self.btnClearCats.Click += self._clear_categories
        self.tbCategoryFilter.TextChanged += self._filter_categories
        self.btnOpenQueue.Click += self._open_queue
        self.btnSaveQueue.Click += self._save_queue
        self.btnCancel.Click += lambda s, e: self.w.Close()
        self.btnOk.Click += self._run

    def _load_categories(self):
        """Загрузить категории модели."""
        cats = get_model_categories()
        self._all_category_items = [CategoryItem(c) for c in cats]
        self._category_items = list(self._all_category_items)
        self.lbCategories.ItemsSource = self._category_items
        self._update_selected_count()

    def _on_group_changed(self, sender=None, args=None):
        """При смене группы параметров."""
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
            except Exception:
                pass

    def _apply_preset(self, preset_list):
        """Применить пресет категорий."""
        preset_ids = set()
        for bic in preset_list:
            try:
                cat = Category.GetCategory(doc, bic)
                if cat:
                    preset_ids.add(cat.Id.IntegerValue)
            except Exception:
                pass

        for item in self._all_category_items:
            item.IsSelected = item.Id in preset_ids

        self._refresh_category_list()
        self._update_selected_count()

    def _clear_categories(self, sender=None, args=None):
        """Очистить выбор категорий."""
        for item in self._all_category_items:
            item.IsSelected = False
        self._refresh_category_list()
        self._update_selected_count()

    def _filter_categories(self, sender=None, args=None):
        """Фильтровать список категорий."""
        filter_text = (self.tbCategoryFilter.Text or u'').strip().lower()
        if filter_text:
            self._category_items = [
                item for item in self._all_category_items
                if filter_text in item.Name.lower()
            ]
        else:
            self._category_items = list(self._all_category_items)
        self.lbCategories.ItemsSource = self._category_items

    def _refresh_category_list(self):
        """Обновить отображение списка категорий."""
        self.lbCategories.ItemsSource = None
        self.lbCategories.ItemsSource = self._category_items

    def _update_selected_count(self):
        """Обновить счётчик выбранных категорий."""
        count = sum(1 for item in self._all_category_items if item.IsSelected)
        self.tbSelectedCount.Text = str(count)

    def _get_selected_categories(self):
        """Получить выбранные категории."""
        return [item for item in self._all_category_items if item.IsSelected]

    def _add_to_queue(self, sender, args):
        """Добавить параметры в очередь."""
        if not self.lbParams.SelectedItems or self.lbParams.SelectedItems.Count == 0:
            forms.alert(u'Выберите параметры слева.')
            return

        selected_cats = self._get_selected_categories()
        if not selected_cats:
            forms.alert(u'Выберите хотя бы одну категорию.')
            return

        gname = self.lbGroups.SelectedItem
        g = self._group_by_name.get(gname)
        if not g:
            return

        bipg_idx = self.cbBipg.SelectedIndex
        bipg = self._bipg_items[bipg_idx][1] if bipg_idx >= 0 else BuiltInParameterGroup.PG_IDENTITY_DATA
        bipg_label = self._bipg_items[bipg_idx][0] if bipg_idx >= 0 else u''
        is_inst = bool(self.rbInst.IsChecked)

        cat_ids = [c.Id for c in selected_cats]
        cat_names = [c.Name for c in selected_cats]

        for pname in list(self.lbParams.SelectedItems):
            extdef = None
            for d in g.Definitions:
                if isinstance(d, ExternalDefinition) and d.Name == pname:
                    extdef = d
                    break
            if extdef is None:
                continue

            # Проверяем что нет дубликата в очереди
            if any(q.Name == extdef.Name for q in self._queue):
                continue

            qi = QueueItem(
                extdef.Name,
                str(extdef.GUID),
                gname,
                is_inst,
                bipg,
                bipg_label,
                cat_ids,
                cat_names
            )
            self._queue.Add(qi)

    def _remove_selected(self, sender, args):
        """Удалить выбранные из очереди."""
        sel = list(self.dgQueue.SelectedItems) if self.dgQueue.SelectedItems else []
        for item in sel:
            self._queue.Remove(item)

    def _save_queue(self, sender, args):
        """Сохранить очередь в JSON."""
        items = [qi.to_json() for qi in self._queue]
        if not items:
            forms.alert(u'Очередь пуста.')
            return
        sfd = SaveFileDialog()
        sfd.Title = u'Сохранить набор параметров'
        sfd.Filter = 'JSON (*.json)|*.json'
        sfd.FileName = 'project_params.json'
        if sfd.ShowDialog() == DialogResult.OK:
            try:
                with open(sfd.FileName, 'wb') as f:
                    f.write(json.dumps(items, indent=2, ensure_ascii=False).encode('utf-8'))
            except Exception as e:
                forms.alert(u'Ошибка сохранения:\n{}'.format(e))

    def _open_queue(self, sender, args):
        """Загрузить очередь из JSON."""
        ofd = OpenFileDialog()
        ofd.Title = u'Открыть набор параметров'
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
                forms.alert(u'Ошибка загрузки:\n{}'.format(e))

    def _resolve_extdef_by_guid(self, guid_str):
        """Найти ExternalDefinition по GUID."""
        try:
            g = Guid(guid_str)
        except Exception:
            return None
        for grp in self._dfile.Groups:
            for d in grp.Definitions:
                try:
                    if isinstance(d, ExternalDefinition) and str(d.GUID) == str(g):
                        return d
                except Exception:
                    pass
        return None

    def _build_category_set(self, category_ids):
        """Построить CategorySet по списку ID."""
        from Autodesk.Revit.DB import ElementId

        cat_set = app.Create.NewCategorySet()
        print(u'      [debug] Строим CategorySet из {} ID'.format(len(category_ids)))

        for cat_id in category_ids:
            try:
                # Ищем категорию по ID в настройках документа
                eid = ElementId(int(cat_id))
                cat = None

                # Метод 1: через Settings.Categories
                for c in doc.Settings.Categories:
                    if c.Id.IntegerValue == int(cat_id):
                        cat = c
                        break

                # Метод 2: через Category.GetCategory
                if cat is None:
                    try:
                        cat = Category.GetCategory(doc, eid)
                    except Exception:
                        pass

                if cat and cat.AllowsBoundParameters:
                    cat_set.Insert(cat)
                else:
                    print(u'      [warn] Категория {} не найдена или не поддерживает параметры'.format(cat_id))

            except Exception as e:
                print(u'      [error] Ошибка для категории {}: {}'.format(cat_id, e))

        print(u'      [debug] CategorySet содержит {} категорий'.format(cat_set.Size))
        return cat_set

    def _run(self, sender, args):
        """Выполнить добавление параметров."""
        items = list(self._queue)
        if not items:
            forms.alert(u'Очередь пуста.')
            return

        try:
            output.activate()
        except Exception:
            pass

        print(u'=' * 60)
        print(u'Добавление параметров в проект: {}'.format(doc.Title))
        print(u'=' * 60)

        total_added = 0
        total_skipped = 0
        total_errors = 0

        for qi in items:
            print(u'\n  {} [{}]'.format(qi.Name, qi.BindingTypeText))
            print(u'    Группирование: {}'.format(qi.BipgText))
            print(u'    Категории ({}): {}'.format(len(qi.CategoryIds), qi.CategoriesText))
            print(u'    GUID: {}'.format(qi.Guid))

            # Находим ExternalDefinition
            extdef = self._resolve_extdef_by_guid(qi.Guid)
            if extdef is None:
                print(u'    [debug] GUID не найден, ищем по имени...')
                # Пробуем найти по имени в группе
                grp = self._group_by_name.get(qi.GroupName)
                if grp:
                    for d in grp.Definitions:
                        if isinstance(d, ExternalDefinition) and d.Name == qi.Name:
                            extdef = d
                            print(u'    [debug] Найден по имени в группе {}'.format(qi.GroupName))
                            break

            if extdef is None:
                print(u'    -> ОШИБКА: не найден в ФОП')
                total_errors += 1
                continue

            print(u'    [debug] ExternalDefinition: {}'.format(extdef.Name))

            # Строим CategorySet
            cat_set = self._build_category_set(qi.CategoryIds)
            if cat_set.Size == 0:
                print(u'    -> ОШИБКА: категории не найдены (IDs: {})'.format(qi.CategoryIds[:5]))
                total_errors += 1
                continue

            # Добавляем параметр
            success, msg = add_param_to_project(extdef, qi.Bipg, qi.IsInstance, cat_set)
            print(u'    -> {}'.format(msg))

            if success:
                total_added += 1
            elif u'существует' in msg.lower():
                total_skipped += 1
            else:
                total_errors += 1

        print(u'\n' + u'=' * 60)
        print(u'ИТОГО: добавлено {}, пропущено {}, ошибок {}'.format(
            total_added, total_skipped, total_errors))
        print(u'=' * 60)

        forms.alert(u'Готово!\n\nДобавлено: {}\nПропущено (уже были): {}\nОшибок: {}'.format(
            total_added, total_skipped, total_errors))

        self.w.Close()


# ========================== RUN ==========================

try:
    ctrl = MainController()
    if ctrl._dfile:
        ctrl.w.ShowDialog()
except Exception as e:
    tb = traceback.format_exc()
    forms.alert(u'Ошибка:\n{}\n\n{}'.format(e, tb))
