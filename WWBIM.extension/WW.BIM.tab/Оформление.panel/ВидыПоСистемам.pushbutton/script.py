# -*- coding: utf-8 -*-
"""Создание 3D-видов по MEP-системам.

Скрипт создаёт отдельные 3D-виды для каждой выбранной системы,
с фильтром по имени системы и 3D-подрезкой границ.
"""

__title__ = "Виды\nпо системам"
__author__ = "WW.BIM"
__doc__ = "Создаёт 3D-виды для выбранных MEP-систем с фильтрами"

import clr
clr.AddReference('System.Windows.Forms')
clr.AddReference('PresentationFramework')
clr.AddReference('PresentationCore')

from System.Collections.Generic import List

from pyrevit import revit, forms
from pyrevit.forms import WPFWindow

from Autodesk.Revit.DB import (
    FilteredElementCollector, 
    BuiltInCategory, 
    View,
    View3D, 
    ViewFamilyType,
    Transaction,
    ElementId,
    BuiltInParameter,
    BoundingBoxXYZ,
    XYZ,
    ParameterFilterElement,
    ElementParameterFilter,
    FilterStringRule,
    FilterStringEquals,
    ParameterValueProvider,
    LogicalOrFilter,
    OverrideGraphicSettings,
    ElementFilter,
    FilterInverseRule,
    ElementMulticategoryFilter,
    LogicalAndFilter
)

try:
    from Autodesk.Revit.DB import FilterStringContains
except ImportError:
    FilterStringContains = None

from Autodesk.Revit.DB.Mechanical import MechanicalSystem
from Autodesk.Revit.DB.Plumbing import PipingSystem
from Autodesk.Revit.DB.Electrical import ElectricalSystem

doc = revit.doc
uidoc = revit.uidoc


# Категории MEP элементов для фильтра
MEP_CATEGORIES = [
    BuiltInCategory.OST_DuctCurves,
    BuiltInCategory.OST_DuctFitting,
    BuiltInCategory.OST_DuctAccessory,
    BuiltInCategory.OST_DuctTerminal,
    BuiltInCategory.OST_DuctInsulations,
    BuiltInCategory.OST_FlexDuctCurves,
    BuiltInCategory.OST_MechanicalEquipment,
    BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_PipeInsulations,
    BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_Sprinklers
]


class SystemItem(object):
    """Класс для хранения данных о системе с поддержкой привязки."""
    
    def __init__(self, system_element, name):
        self._system = system_element
        self._name = name
        self._is_selected = False
    
    @property
    def Name(self):
        return self._name
    
    @property
    def System(self):
        return self._system
    
    @property
    def IsSelected(self):
        return self._is_selected
    
    @IsSelected.setter
    def IsSelected(self, value):
        self._is_selected = value


class ViewsBySystemsWindow(WPFWindow):
    """Главное окно для создания видов по системам."""
    
    def __init__(self):
        WPFWindow.__init__(self, 'ViewsBySystemsWindow.xaml')
        
        self._is_initialized = False
        
        # Категории систем
        self.categories = {
            'Воздуховоды': self._get_duct_systems,
            'Трубопроводы': self._get_pipe_systems,
            'Кабельные лотки': self._get_cable_tray_systems,
            'Все MEP-системы': self._get_all_systems
        }
        
        self.systems = []
        self.available_params = None
        self.selected_param_id = None
        
        # Сначала инициализируем параметры и шаблоны
        self._init_system_params()
        self._init_templates()
        
        # В самом конце инициализируем категории, что вызовет обновление списка
        self._is_initialized = True
        self._init_categories()
        
    def _update_systems_list(self):
        """Обновление списка систем при смене категории."""
        if not self._is_initialized:
            return
            
        category_name = self.cb_category.SelectedItem
        if not category_name:
            return
        
        getter = self.categories.get(category_name)
        if not getter:
            return
        
        try:
            raw_systems = getter()
            
            # Убираем дубликаты по имени и сортируем
            unique_names = {}
            for system, name in raw_systems:
                if name and name not in unique_names:
                    unique_names[name] = system
            
            self.systems = [SystemItem(sys, name) for name, sys in sorted(unique_names.items())]
            self.lb_systems.ItemsSource = self.systems
        except Exception:
            self.systems = []
            self.lb_systems.ItemsSource = None
            
    def _init_system_params(self):
        """Определение доступных параметров системы."""
        # Словарь: {отображаемое имя: (имя параметра для поиска, ElementId или None)}
        self.available_params = {}
        
        # Пробуем найти параметр ADSK_Система_Имя
        adsk_param = self._find_shared_parameter("ADSK_Система_Имя")
        if adsk_param:
            self.available_params["ADSK_Система_Имя"] = ("ADSK_Система_Имя", adsk_param)
            self.cb_system_param.Items.Add("ADSK_Система_Имя")
        
        # Пробуем найти параметр ИмяСистемы
        name_param = self._find_shared_parameter("ИмяСистемы")
        if name_param:
            self.available_params["ИмяСистемы"] = ("ИмяСистемы", name_param)
            self.cb_system_param.Items.Add("ИмяСистемы")
        
        # Всегда добавляем встроенный параметр как запасной вариант
        self.available_params["Имя системы (встроенный)"] = ("RBS_SYSTEM_NAME_PARAM", ElementId(BuiltInParameter.RBS_SYSTEM_NAME_PARAM))
        self.cb_system_param.Items.Add("Имя системы (встроенный)")
        
        # Проверяем доступность параметров
        if "ADSK_Система_Имя" in self.available_params:
            self.cb_system_param.SelectedIndex = 0
        elif "ИмяСистемы" in self.available_params:
            self.cb_system_param.SelectedIndex = 0
        else:
            self.cb_system_param.SelectedIndex = self.cb_system_param.Items.Count - 1
    
    def _find_shared_parameter(self, param_name):
        """Поиск общего параметра по имени."""
        # Пробуем найти в ProjectParameters через DefinitionBindingMapIterator
        try:
            iterator = doc.ParameterBindings.ForwardIterator()
            while iterator.MoveNext():
                definition = iterator.Key
                if definition and definition.Name == param_name:
                    # Пробуем получить ID параметра из первого элемента
                    binding = iterator.Current
                    if binding:
                        # Ищем элемент с этим параметром
                        for cat in MEP_CATEGORIES:
                            try:
                                collector = FilteredElementCollector(doc).OfCategory(cat).WhereElementIsNotElementType()
                                first_elem = collector.FirstElement()
                                if first_elem:
                                    param = first_elem.LookupParameter(param_name)
                                    if param:
                                        return param.Id
                            except Exception:
                                continue
        except Exception:
            pass
        
        # Пробуем поискать в элементах проекта напрямую
        for cat in MEP_CATEGORIES:
            try:
                collector = FilteredElementCollector(doc).OfCategory(cat).WhereElementIsNotElementType()
                first_elem = collector.FirstElement()
                if first_elem:
                    param = first_elem.LookupParameter(param_name)
                    if param:
                        return param.Id
            except Exception:
                continue
        
        return None
    
    def _get_categories_for_parameter(self, param_id, param_name):
        """Определение категорий, к которым применим параметр."""
        
        # Если это встроенный параметр - возвращаем все MEP категории
        try:
            bip = param_id.IntegerValue
            if bip == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
                return MEP_CATEGORIES
        except Exception:
            pass
        
        # Для общих параметров проверяем привязку через ParameterBindings
        applicable_categories = []
        
        try:
            iterator = doc.ParameterBindings.ForwardIterator()
            found_binding = False
            while iterator.MoveNext():
                definition = iterator.Key
                if definition and definition.Name == param_name:
                    found_binding = True
                    binding = iterator.Current
                    if binding:
                        # Получаем категории из привязки
                        categories = binding.Categories
                        for cat in categories:
                            try:
                                cat_id_int = cat.Id.IntegerValue
                                
                                # Проверяем, есть ли эта категория в нашем списке MEP
                                # Сравниваем по числовому значению
                                found = False
                                for mep_cat in MEP_CATEGORIES:
                                    if int(mep_cat) == cat_id_int:
                                        applicable_categories.append(mep_cat)
                                        found = True
                                        break
                                
                            except Exception:
                                continue
                    break
            
        except Exception:
            pass
        
        # Если не нашли через binding, проверяем элементы напрямую
        if not applicable_categories:
            for cat in MEP_CATEGORIES:
                try:
                    collector = FilteredElementCollector(doc).OfCategory(cat).WhereElementIsNotElementType()
                    first_elem = collector.FirstElement()
                    if first_elem:
                        param = first_elem.get_Parameter(param_id)
                        if param:
                            applicable_categories.append(cat)
                except Exception:
                    continue
        
        result = applicable_categories if applicable_categories else MEP_CATEGORIES
        return result
    
    def _init_categories(self):
        """Инициализация выпадающего списка категорий."""
        for cat_name in self.categories.keys():
            self.cb_category.Items.Add(cat_name)
        self.cb_category.SelectedIndex = 0
    
    def _init_templates(self):
        """Загрузка шаблонов 3D-видов из проекта."""
        templates = FilteredElementCollector(doc)\
            .OfClass(View3D)\
            .WhereElementIsNotElementType()\
            .ToElements()
        
        self.cb_template.Items.Add("(без шаблона)")
        self.templates = [None]
        
        for view in templates:
            if view.IsTemplate:
                self.cb_template.Items.Add(view.Name)
                self.templates.append(view)
        
        self.cb_template.SelectedIndex = 0
    
    def _get_current_param_info(self):
        """Получение информации о текущем выбранном параметре."""
        selected_param_name = self.cb_system_param.SelectedItem
        
        if not selected_param_name or selected_param_name not in self.available_params:
            return "RBS_SYSTEM_NAME_PARAM", ElementId(BuiltInParameter.RBS_SYSTEM_NAME_PARAM), True
        
        p_name, p_id = self.available_params[selected_param_name]
        
        is_builtin = False
        try:
            bip = p_id.IntegerValue
            if bip == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
                is_builtin = True
        except Exception:
            pass
            
        return p_name, p_id, is_builtin

    
    def _collect_unique_param_values(self, categories, p_name, p_id, is_builtin):
        """Сбор уникальных значений параметра из элементов категорий."""
        unique_values = set()
        
        # Оптимизация: сканируем только основные элементы (Curves), 
        # так как аксессуары и фитинги обычно наследуют систему
        target_cats = []
        for cat in categories:
            try:
                target_cats.append(ElementId(cat))
            except:
                pass
            
        if not target_cats:
            return []
            
        # Используем ElementMulticategoryFilter
        cat_filter = ElementMulticategoryFilter(List[ElementId](target_cats))
        
        collector = FilteredElementCollector(doc)\
            .WherePasses(cat_filter)\
            .WhereElementIsNotElementType()
        
        
        for elem in collector:
            val = None
            try:
                if is_builtin:
                    p = elem.get_Parameter(p_id)
                else:
                    p = elem.LookupParameter(p_name)
                
                if p and p.HasValue:
                    val = p.AsString()
            except Exception:
                continue
                
            if val:
                unique_values.add(val)
                
        return sorted(list(unique_values))

    def _get_duct_systems(self):
        """Получение систем воздуховодов."""
        p_name, p_id, is_builtin = self._get_current_param_info()
        
        # Если это встроенное имя системы - берем из самих систем (быстро и надежно)
        if is_builtin and p_id.IntegerValue == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
            systems = FilteredElementCollector(doc)\
                .OfClass(MechanicalSystem)\
                .ToElements()
            # Сортируем по имени
            sorted_systems = sorted(systems, key=lambda s: s.Name)
            return [(s, s.Name) for s in sorted_systems if s.Name]
            
        # Иначе сканируем элементы
        cats = [
            BuiltInCategory.OST_DuctCurves,
            BuiltInCategory.OST_FlexDuctCurves
        ]
        names = self._collect_unique_param_values(cats, p_name, p_id, is_builtin)
        return [(None, name) for name in names]
    
    def _get_pipe_systems(self):
        """Получение систем трубопроводов."""
        p_name, p_id, is_builtin = self._get_current_param_info()
        
        # Если это встроенное имя системы - берем из самих систем
        if is_builtin and p_id.IntegerValue == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
            systems = FilteredElementCollector(doc)\
                .OfClass(PipingSystem)\
                .ToElements()
            sorted_systems = sorted(systems, key=lambda s: s.Name)
            return [(s, s.Name) for s in sorted_systems if s.Name]
            
        # Иначе сканируем элементы
        cats = [
            BuiltInCategory.OST_PipeCurves,
            BuiltInCategory.OST_FlexPipeCurves
        ]
        names = self._collect_unique_param_values(cats, p_name, p_id, is_builtin)
        return [(None, name) for name in names]
    
    def _get_cable_tray_systems(self):
        """Получение систем кабельных лотков."""
        p_name, p_id, is_builtin = self._get_current_param_info()
        
        # Если это встроенное имя системы - берем из самих систем
        if is_builtin and p_id.IntegerValue == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
            systems = FilteredElementCollector(doc)\
                .OfClass(ElectricalSystem)\
                .ToElements()
            sorted_systems = sorted(systems, key=lambda s: s.Name)
            return [(s, s.Name) for s in sorted_systems if s.Name]
            
        # Иначе сканируем элементы
        cats = [
            BuiltInCategory.OST_CableTray,
            BuiltInCategory.OST_Conduit
        ]
        names = self._collect_unique_param_values(cats, p_name, p_id, is_builtin)
        return [(None, name) for name in names]

    def _get_all_systems(self):
        """Получение всех MEP-систем."""
        all_systems = []
        all_systems.extend(self._get_duct_systems())
        all_systems.extend(self._get_pipe_systems())
        all_systems.extend(self._get_cable_tray_systems())
        return all_systems
    
    def _update_systems_list(self):
        """Обновление списка систем при смене категории."""
        if not self._is_initialized:
            return
            
        category_name = self.cb_category.SelectedItem
        if not category_name:
            return
        
        getter = self.categories.get(category_name)
        if not getter:
            return
        
        try:
            raw_systems = getter()
            
            # Убираем дубликаты по имени и сортируем
            unique_names = {}
            for system, name in raw_systems:
                if name and name not in unique_names:
                    unique_names[name] = system
            
            self.systems = [SystemItem(sys, name) for name, sys in sorted(unique_names.items())]
            self.lb_systems.ItemsSource = self.systems
        except Exception:
            self.systems = []
            self.lb_systems.ItemsSource = None
    
    def category_changed(self, sender, e):
        """Обработчик изменения категории."""
        self._update_systems_list()
        
    def system_param_changed(self, sender, e):
        """Обработчик изменения параметра системы."""
        self._update_systems_list()
    

    
    def select_all(self, sender, e):
        """Выбрать все системы."""
        for item in self.systems:
            item.IsSelected = True
        self.lb_systems.Items.Refresh()
    
    def select_none(self, sender, e):
        """Снять выбор со всех систем."""
        for item in self.systems:
            item.IsSelected = False
        self.lb_systems.Items.Refresh()
    
    def select_invert(self, sender, e):
        """Инвертировать выбор."""
        for item in self.systems:
            item.IsSelected = not item.IsSelected
        self.lb_systems.Items.Refresh()
    
    def _get_3d_view_type(self):
        """Получение типа 3D-вида для создания."""
        view_types = FilteredElementCollector(doc)\
            .OfClass(ViewFamilyType)\
            .ToElements()
        
        for vt in view_types:
            if vt.ViewFamily.ToString() == 'ThreeDimensional':
                return vt
        return None
    
    def _generate_unique_name(self, base_name, existing_names):
        """Генерация уникального имени вида."""
        if base_name not in existing_names:
            return base_name
        
        counter = 1
        while True:
            new_name = "{} ({})".format(base_name, counter)
            if new_name not in existing_names:
                return new_name
            counter += 1
    
    def _get_existing_view_names(self):
        """Получение имён существующих видов."""
        views = FilteredElementCollector(doc)\
            .OfClass(View3D)\
            .WhereElementIsNotElementType()\
            .ToElements()
        return set(v.Name for v in views)
    
    def _get_existing_filter_names(self):
        """Получение имён существующих фильтров."""
        filters = FilteredElementCollector(doc)\
            .OfClass(ParameterFilterElement)\
            .ToElements()
        return set(f.Name for f in filters)
    
    def _get_system_elements(self, system_obj, system_name, param_name, param_id):
        """Получение всех элементов системы."""
        
        # Если есть объект системы - используем быстрый способ
        if system_obj:
            element_ids = []
            try:
                if hasattr(system_obj, 'DuctNetwork'):
                    elements = system_obj.DuctNetwork
                    if elements:
                        element_ids.extend([e.Id for e in elements])
                
                if hasattr(system_obj, 'PipingNetwork'):
                    elements = system_obj.PipingNetwork
                    if elements:
                        element_ids.extend([e.Id for e in elements])
                
                if hasattr(system_obj, 'GetDependentElements'):
                    deps = system_obj.GetDependentElements(None)
                    if deps:
                        element_ids.extend(deps)
            except Exception:
                pass
            
            if system_obj.Id not in element_ids:
                element_ids.append(system_obj.Id)
            
            return list(set(element_ids))
            
        # Иначе ищем по параметру
        try:
            # Создаем фильтр по параметру = system_name
            from Autodesk.Revit.DB import (
                ParameterFilterRuleFactory, 
                ElementParameterFilter,
                FilterStringRule,
                FilterStringEquals
            )
            
            elem_filter = None
            try:
                rule = ParameterFilterRuleFactory.CreateEqualsRule(param_id, system_name, False)
                elem_filter = ElementParameterFilter(rule)
            except Exception:
                # Fallback
                provider = ParameterValueProvider(param_id)
                evaluator = FilterStringEquals()
                rule = FilterStringRule(provider, evaluator, system_name, False)
                elem_filter = ElementParameterFilter(rule)
            
            found_ids = []
            cats_to_check = MEP_CATEGORIES
            
            for cat in cats_to_check:
                try:
                    collector = FilteredElementCollector(doc)\
                        .OfCategory(cat)\
                        .WhereElementIsNotElementType()\
                        .WherePasses(elem_filter)
                    
                    ids = collector.ToElementIds()
                    if ids:
                        found_ids.extend(ids)
                except Exception:
                    continue
            
            return found_ids
            
        except Exception:
            return []

    
    def _calculate_bounding_box(self, element_ids):
        """Вычисление охватывающего BoundingBox для элементов."""
        min_pt = None
        max_pt = None
        
        for eid in element_ids:
            elem = doc.GetElement(eid)
            if elem is None:
                continue
            
            bb = elem.get_BoundingBox(None)
            if bb is None:
                continue
            
            if min_pt is None:
                min_pt = XYZ(bb.Min.X, bb.Min.Y, bb.Min.Z)
                max_pt = XYZ(bb.Max.X, bb.Max.Y, bb.Max.Z)
            else:
                min_pt = XYZ(
                    min(min_pt.X, bb.Min.X),
                    min(min_pt.Y, bb.Min.Y),
                    min(min_pt.Z, bb.Min.Z)
                )
                max_pt = XYZ(
                    max(max_pt.X, bb.Max.X),
                    max(max_pt.Y, bb.Max.Y),
                    max(max_pt.Z, bb.Max.Z)
                )
        
        if min_pt is None or max_pt is None:
            return None
        
        # Добавляем небольшой отступ
        offset = 1.0  # ~0.3 метра
        min_pt = XYZ(min_pt.X - offset, min_pt.Y - offset, min_pt.Z - offset)
        max_pt = XYZ(max_pt.X + offset, max_pt.Y + offset, max_pt.Z + offset)
        
        section_box = BoundingBoxXYZ()
        section_box.Min = min_pt
        section_box.Max = max_pt
        
        return section_box
    
    def _get_other_systems_in_box(self, section_box, allowed_systems, param_id, param_name):
        """Проверка наличия элементов других систем в BoundingBox."""
        # Создаём Outline для фильтрации
        from Autodesk.Revit.DB import Outline, BoundingBoxIntersectsFilter
        
        outline = Outline(section_box.Min, section_box.Max)
        bb_filter = BoundingBoxIntersectsFilter(outline)
        
        # Подготавливаем набор разрешенных имен
        if isinstance(allowed_systems, (list, tuple, set)):
            allowed_set = set(s.strip() for s in allowed_systems)
        else:
            allowed_set = {allowed_systems.strip()}
            
        # Определяем, встроенный ли параметр
        is_builtin = False
        try:
            val = param_id.IntegerValue
            if val == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
                is_builtin = True
        except:
             pass
        
        other_systems_found = []
        total_checked = 0
        elements_with_param = 0
        
        # Собираем элементы всех MEP категорий в пределах box
        for cat in MEP_CATEGORIES:
            try:
                elements = FilteredElementCollector(doc)\
                    .OfCategory(cat)\
                    .WhereElementIsNotElementType()\
                    .WherePasses(bb_filter)\
                    .ToElements()
                
                cat_count = len(list(elements))
                if cat_count > 0:
                    pass
                
                for elem in elements:
                    total_checked += 1
                    # Проверяем имя системы элемента
                    sys_param = None
                    if is_builtin:
                        # Для встроенного параметра используем BuiltInParameter
                        try:
                            # Хардкод для Имени Системы (самый частый кейс)
                            if param_id.IntegerValue == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
                                sys_param = elem.get_Parameter(BuiltInParameter.RBS_SYSTEM_NAME_PARAM)
                            else:
                                bip_enum = BuiltInParameter(param_id.IntegerValue)
                                sys_param = elem.get_Parameter(bip_enum)
                        except Exception:
                            sys_param = None
                    else:
                        # Для общего параметра используем имя (надежнее, чем Guid, если id вдруг не тот)
                        sys_param = elem.LookupParameter(param_name)
                    
                    if sys_param and sys_param.HasValue:
                        elements_with_param += 1
                        elem_sys_name = sys_param.AsString()
                        if elem_sys_name:
                            # Если имя элемента (без пробелов) не входит в список разрешенных - это чужой
                            if elem_sys_name.strip() not in allowed_set:
                                if elem_sys_name not in other_systems_found:
                                    other_systems_found.append(elem_sys_name)
                                return True  # Найден элемент другой системы
            except Exception:
                continue
        
        return False
    
    def _create_system_filter(self, system_name, filter_name, param_id, param_name):
        """Создание фильтра "Имя системы не равно значению"."""
        from Autodesk.Revit.DB import (
            FilteredElementCollector,
            ParameterFilterRuleFactory,
            ElementFilter
        )
        
        
        # Определяем применимые категории для параметра
        applicable_cats = self._get_categories_for_parameter(param_id, param_name)
        
        # Категории для фильтра
        cat_ids = List[ElementId]()
        for cat in applicable_cats:
            try:
                cat_ids.Add(ElementId(cat))
            except Exception:
                continue
        
        
        if cat_ids.Count == 0:
            return None
        
        # Создаём правило фильтра: Имя системы НЕ равно system_name
        try:
            # Используем ParameterFilterRuleFactory для создания правила "не равно"
            rule = ParameterFilterRuleFactory.CreateNotEqualsRule(param_id, system_name, False)
            elem_filter = ElementParameterFilter(rule)
        except Exception as e1:
            try:
                # Альтернативный способ через инверсию
                provider = ParameterValueProvider(param_id)
                evaluator = FilterStringEquals()
                # Пробуем новый API (без caseSensitive)
                try:
                    rule = FilterStringRule(provider, evaluator, system_name)
                except TypeError:
                    # Старый API (с caseSensitive)
                    rule = FilterStringRule(provider, evaluator, system_name, False)
                inverse_rule = FilterInverseRule(rule)
                elem_filter = ElementParameterFilter(inverse_rule)
            except Exception as e2:
                return None
        
        # Создаём ParameterFilterElement
        try:
            param_filter = ParameterFilterElement.Create(doc, filter_name, cat_ids, elem_filter)
            return param_filter
        except Exception:
            return None
    
    def _apply_filter_to_view(self, view, param_filter):
        """Применение фильтра к виду с выключением видимости."""
        try:
            view.AddFilter(param_filter.Id)
            view.SetFilterVisibility(param_filter.Id, False)
            return True
        except Exception:
            return False
    
    
    def _create_multi_system_filter(self, system_names, filter_name, param_id, param_name):
        """Создание фильтра для нескольких систем (скрыть всё, кроме них)."""
        from Autodesk.Revit.DB import (
            ParameterFilterRuleFactory,
            ElementParameterFilter,
            FilterStringRule,
            FilterStringEquals,
            LogicalAndFilter
        )

        try:
            # Создаем правила "НЕ РАВНО" для каждой системы
            filters = []
            
            # Определяем, встроенный параметр или общий
            is_builtin = False
            try:
                bip = param_id.IntegerValue
                if bip == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
                    is_builtin = True
            except:
                pass

            for sys_name in system_names:
                f_rule = None
                if is_builtin:
                    # Для встроенных
                    f_rule = ParameterFilterRuleFactory.CreateNotEqualsRule(param_id, sys_name, False)
                else:
                    # Для общих параметров правило NotEquals
                    # Но ParameterFilterRuleFactory.CreateNotEqualsRule работает и с ID общих параметров?
                    # Да, должен.
                    f_rule = ParameterFilterRuleFactory.CreateNotEqualsRule(param_id, sys_name, False)
                
                if f_rule:
                    filters.append(ElementParameterFilter(f_rule))

            if not filters:
                return None

            # Объединяем через AND (Скрыть если != Сис1 И != Сис2 И ...)
            and_filter = LogicalAndFilter(List[ElementFilter](filters))
            
            # Подготавливаем категории
            cat_ids = List[ElementId]()
            for bic in MEP_CATEGORIES:
                cat_ids.Add(ElementId(bic))
            
            # Создаем ParameterFilterElement
            return ParameterFilterElement.Create(doc, filter_name, cat_ids, and_filter)

        except Exception:
            return None

    def create_views(self, sender, e):
        """Создание видов для выбранных систем."""
        selected_items = [item for item in self.systems if item.IsSelected]
        
        if not selected_items:
            forms.alert("Не выбрана ни одна система!", title="Внимание")
            return
        
        # Получаем данные из UI
        template = None
        if self.cb_template.SelectedIndex > 0:
            template = self.templates[self.cb_template.SelectedIndex]
            
        prefix = self.tb_prefix.Text
        skip_filter = self.chk_skip_filter.IsChecked
        open_views = self.chk_open.IsChecked
        combined_view = self.ck_combined_view.IsChecked
        
        # Helper to get current parameter info
        def _get_current_param_info(self):
            selected_param_name = self.cb_system_param.SelectedItem
            if not selected_param_name or selected_param_name not in self.available_params:
                forms.alert("Не выбран параметр системы.", title="Ошибка")
                return None, None, False # Return default values
            
            param_name, param_id = self.available_params[selected_param_name]
            
            is_builtin = False
            try:
                bip = param_id.IntegerValue
                if bip == int(BuiltInParameter.RBS_SYSTEM_NAME_PARAM):
                    is_builtin = True
            except:
                pass
            return param_name, param_id, is_builtin

        param_name, param_id, is_builtin = _get_current_param_info(self)
        if param_name is None: # Check if param retrieval failed
            return
        
        created_views = []
        
        t = Transaction(doc, "Создание видов по системам")
        t.Start()
        
        try:
            # Словарь существующих видов для поиска
            all_views = FilteredElementCollector(doc).OfClass(View).ToElements()
            existing_views_dict = {v.Name: v for v in all_views}
            
            # === Режим СВОДНОГО ВИДА ===
            if combined_view:
                # Собираем имена и элементы всех систем
                all_sys_names = []
                all_element_ids = []
                
                for item in selected_items:
                    sys_name = item.Name
                    all_sys_names.append(sys_name)
                    # Собираем элементы
                    ids = self._get_system_elements(item.System, sys_name, param_name, param_id)
                    all_element_ids.extend(ids)
                
                all_element_ids = list(set(all_element_ids))
                
                # Формируем имя вида из имен систем
                joined_names = "_".join(all_sys_names)
                if len(joined_names) > 50:
                     joined_names = joined_names[:47] + "..."
                     
                view_name = "{}{}".format(prefix, joined_names)
                
                # Ищем существующий вид или создаем новый
                view = existing_views_dict.get(view_name)
                
                if not view:
                    view_type_id = self._get_3d_view_type().Id
                    view = View3D.CreateIsometric(doc, view_type_id)
                    view.Name = view_name
                
                # Применяем шаблон
                if template:
                    view.ViewTemplateId = template.Id
                
                # 3D-подрезка
                if all_element_ids:
                    section_box = self._calculate_bounding_box(all_element_ids)
                    if section_box:
                        view.SetSectionBox(section_box)
                        view.IsSectionBoxActive = True
                
                # Логика фильтров (нужен или нет)
                need_filter = True
                if skip_filter and section_box:
                    # Проверяем, есть ли чужие элементы в боксе
                    has_others = self._get_other_systems_in_box(section_box, all_sys_names, param_id, param_name)
                    if not has_others:
                        need_filter = False
                
                if need_filter:
                    # Создаем мульти-фильтр
                    # Имя фильтра: СисНе_Sys1_Sys2
                    filter_name = "СисНе_{}".format(joined_names)
                    
                    existing_filters = FilteredElementCollector(doc).OfClass(ParameterFilterElement).ToElements()
                    # Ищем фильтр с таким именем
                    pfe = None
                    for f in existing_filters:
                        if f.Name == filter_name:
                            pfe = f
                            break
                    
                    if not pfe:
                        # Создаем новый
                        pfe = self._create_multi_system_filter(all_sys_names, filter_name, param_id, param_name)
                    
                    if pfe:
                        if not view.IsFilterApplied(pfe.Id):
                            try:
                                view.AddFilter(pfe.Id)
                            except:
                                pass
                        view.SetFilterVisibility(pfe.Id, False)
                    
                created_views.append(view)
                
            # === Режим ОТДЕЛЬНЫХ ВИДОВ (По старому) ===
            else:
                for sys_item in selected_items:
                    system_name = sys_item.Name
                    
                    # Имя вида
                    view_name = "{}{}".format(prefix, system_name)
                    
                    # Ищем существующий или создаем
                    view = existing_views_dict.get(view_name)
                    
                    if not view:
                        view_type_id = self._get_3d_view_type().Id
                        view = View3D.CreateIsometric(doc, view_type_id)
                        view.Name = view_name
                    
                    # Применяем шаблон
                    if template:
                        view.ViewTemplateId = template.Id
                    
                    # Получаем элементы системы
                    element_ids = self._get_system_elements(sys_item.System, system_name, param_name, param_id)
                    
                    # Рассчитываем BoundingBox для 3D-подрезки
                    section_box = self._calculate_bounding_box(element_ids)
                    if section_box:
                        view.SetSectionBox(section_box)
                        view.IsSectionBoxActive = True
                    
                    # Логика фильтров (нужен или нет)
                    need_filter = True
                    if skip_filter and section_box:
                        # Проверяем, есть ли чужие элементы в боксе
                        has_others = self._get_other_systems_in_box(section_box, system_name, param_id, param_name)
                        if not has_others:
                            need_filter = False
                    
                    if need_filter:
                        # Создаём или ищем фильтр
                        filter_name_prefix = "СисНе_"
                        filter_name = "{}{}".format(filter_name_prefix, system_name)
                        
                        # Проверяем, существует ли такой фильтр
                        existing_filters = FilteredElementCollector(doc).OfClass(ParameterFilterElement).ToElements()
                        target_filter = None
                        for f in existing_filters:
                            if f.Name == filter_name:
                                target_filter = f
                                break
                        
                        if not target_filter:
                            target_filter = self._create_system_filter(system_name, filter_name, param_id, param_name)
                        
                        if target_filter:
                            try:
                                if not view.IsFilterApplied(target_filter.Id):
                                    view.AddFilter(target_filter.Id)
                                view.SetFilterVisibility(target_filter.Id, False)
                            except Exception:
                                pass
                    else:
                        pass
                        
                    created_views.append(view)
                
            t.Commit()
            
            if open_views and created_views:
                uidoc.ActiveView = created_views[0]
                
            self.Close()
            
        except Exception as ex:
            t.RollBack()
            forms.alert("Ошибка при создании видов:\n{}".format(ex), title="Ошибка")
    
    def cancel_click(self, sender, e):
        """Закрытие окна."""
        self.Close()


# Запуск
if __name__ == '__main__':
    window = ViewsBySystemsWindow()
    window.ShowDialog()
