# -*- coding: utf-8 -*-
import sys
import clr

clr.AddReference('ProtoGeometry')
clr.AddReference("RevitNodes")
clr.AddReference("RevitServices")
clr.AddReference("RevitAPI")
clr.AddReference("RevitAPIUI")
clr.AddReference("dosymep.Revit.dll")
clr.AddReference("dosymep.Bim4Everyone.dll")

import Revit
import dosymep
import codecs
import math

clr.ImportExtensions(Revit.Elements)
clr.ImportExtensions(Revit.GeometryConversion)

import System
import JsonOperatorLib
import DebugPlacerLib
from System.Collections.Generic import *

from Autodesk.Revit.DB import *
from Autodesk.Revit.DB import InternalOrigin
from Autodesk.Revit.UI.Selection import Selection
from Autodesk.DesignScript.Geometry import *

import RevitServices
from RevitServices.Persistence import DocumentManager
from RevitServices.Transactions import TransactionManager

from pyrevit import forms
from pyrevit import revit
from pyrevit import script
from pyrevit import HOST_APP
from pyrevit import EXEC_PARAMS
from rpw.ui.forms import select_file

clr.ImportExtensions(dosymep.Revit)
clr.ImportExtensions(dosymep.Bim4Everyone)
from dosymep.Bim4Everyone.Templates import ProjectParameters
from dosymep_libs.bim4everyone import *

doc = __revit__.ActiveUIDocument.Document  # type: Document
uiapp = __revit__.Application
uidoc = __revit__.ActiveUIDocument


EQUIPMENT_TYPE_NAME = "Оборудование"
VALVE_TYPE_NAME = "Клапан"
OUTER_VALVE_NAME = "ZAWTERM"
FAMILY_NAME_CONST = 'Обр_ОП_Универсальный'
DEBUG_MODE = False
EPSILON = 1e-9


class CylinderZ:
    def __init__(self, z_min, z_max):
        self.radius = 1000
        self.z_min = z_min
        self.z_max = z_max
        self.cylinder_len = z_max - z_min

    def contains_point(self, point):
        """Проверяет, находится ли точка внутри цилиндра по Z"""
        return self.z_min <= point.Z <= self.z_max


class UnitConverter:
    @staticmethod
    def to_millimeters(value):
        """Конвертирует внутренние единицы Revit в миллиметры."""
        return UnitUtils.ConvertFromInternalUnits(value, UnitTypeId.Millimeters)

    @staticmethod
    def from_meters(value):
        """Конвертирует внутренние единицы Revit в метры."""
        return UnitUtils.ConvertToInternalUnits(value, UnitTypeId.Meters)

    @staticmethod
    def to_watts(value):
        """Конвертирует внутренние единицы Revit в ватты."""
        return UnitUtils.ConvertToInternalUnits(value, UnitTypeId.Watts)

    @staticmethod
    def meters_to_millimeters(value):
        """Конвертирует метры в миллиметры."""
        return UnitUtils.Convert(value, UnitTypeId.Meters, UnitTypeId.Millimeters)


class TextParser:
    @staticmethod
    def parse_float(value):
        """Конвертирует строку в float, заменяя запятые на точки."""
        return float(value.replace(',', '.'))

    @staticmethod
    def parse_setting(value):
        """Обрабатывает значение настройки (например, 'N' → 0)."""
        if value in ('N', '', 'Kvs'):
            return 0

        return TextParser.parse_float(value)


class LevelCylinderGenerator:
    MAX_Z_OFFSET = 2500  # Максимальная высота цилиндра
    Z_STOCK = 250  # Запас по высоте

    @classmethod
    def create_cylinders(cls, equipment_list):
        """Создает CylinderZ для каждого уровня оборудования."""
        z_values = {eq.rotated_coords.Z for eq in equipment_list if eq.type_name == EQUIPMENT_TYPE_NAME}
        z_values = sorted(z_values)

        cylinders = []
        for i, z in enumerate(z_values):
            z_min = z - cls.Z_STOCK
            z_max = z_min + cls.MAX_Z_OFFSET if (i == len(z_values) - 1) \
                else min(z_min + cls.MAX_Z_OFFSET, z_values[i + 1] - cls.Z_STOCK)
            cylinders.append(CylinderZ(z_min, z_max))

        return cylinders


class BasePointHelper:
    _base_point = None
    _base_point_z = None

    @classmethod
    def get_base_point(cls, doc):
        if cls._base_point is None:
            cls._base_point = FilteredElementCollector(doc) \
                .OfCategory(BuiltInCategory.OST_ProjectBasePoint) \
                .WhereElementIsNotElementType() \
                .FirstElement()

        return cls._base_point

    @classmethod
    def get_base_point_z(cls, doc):
        if cls._base_point_z is None:
            base_point = cls.get_base_point(doc)
            cls._base_point_z = base_point.GetParamValue(BuiltInParameter.BASEPOINT_ELEVATION_PARAM)

        return cls._base_point_z


class AuditorEquipment:
    '''
    Класс используется для хранения и обратки информации об элементах из Аудитора.

    ---
    processed : Bool
        Булева переменная предназначенная для недопущения дублирования элементов из Аудитор в циклах обработки

    level_cylinder : list
        Список, который содержит пары Z min и Z max для каждого элемента из Аудитор

    '''
    processed = False
    level_cylinder = None

    def __init__(self,
                 connection_type="",
                 rotated_coords=None,
                 original_coords=None,
                 equipment_len=0,
                 code="",
                 real_power=0,
                 nominal_power=0,
                 setting=0.0,
                 maker="",
                 full_name="",
                 type_name=None):
        '''
        Parametrs
        --------
        connection_type : str
            Тип обрабатываемого элемента в Аудиторе
        '''
        self.base_point_z = BasePointHelper.get_base_point_z(doc)
        self.connection_type = connection_type
        self.original_coords = original_coords or XYZ.Zero
        self.rotated_coords = rotated_coords or XYZ.Zero
        self.equipment_len = equipment_len
        self.code = code
        self.real_power = real_power
        self.nominal_power = nominal_power
        self.setting = setting
        self.maker = maker
        self.full_name = full_name
        self.type_name = type_name

    def is_in_data_area(self, revit_equipment):
        '''
        Определяет, пересекаются ли области положений элемента в ревите и в аудиторе
        '''
        revit_location = revit_equipment.Location.Point
        revit_bb = revit_equipment.GetBoundingBox()
        revit_bb_center = get_bb_center(revit_bb)
        revit_coords = XYZ(
            UnitConverter.to_millimeters(revit_location.X),
            UnitConverter.to_millimeters(revit_location.Y),
            UnitConverter.to_millimeters(revit_location.Z)
        )
        revit_bb_coords = XYZ(
            UnitConverter.to_millimeters(revit_bb_center.X),
            UnitConverter.to_millimeters(revit_bb_center.Y),
            UnitConverter.to_millimeters(revit_bb_center.Z)
        )
        radius = self.level_cylinder.radius
        if ((abs(self.level_cylinder.z_min - revit_coords.Z) <= EPSILON or self.level_cylinder.z_min < revit_coords.Z)
                and (abs(revit_coords.Z - self.level_cylinder.z_max) <= EPSILON
                     or revit_coords.Z < self.level_cylinder.z_max)):
            distance_to_location_center = self.rotated_coords.DistanceTo(revit_coords)
            distance_to_bb_center = self.rotated_coords.DistanceTo(revit_bb_coords)
            distance = min(distance_to_bb_center, distance_to_location_center)

            return distance <= radius

        return False

    def set_level_cylinder(self, level_cylinders):
        '''
        Вписывает в список свойств элемента из Аудитора минимальную и максимальную отметку проверочного цилиндра.
        При активации DEBUG_MODE создает в модели экземпляр Цилиндра по координатам элемента в Аудиторе.
        '''
        for level_cylinder in level_cylinders:
            if level_cylinder.contains_point(self.rotated_coords):
                self.level_cylinder = level_cylinder

                if DEBUG_MODE:
                    comment = "{};{};{};{}".format(
                        self.type_name,
                        self.rotated_coords.X,
                        self.rotated_coords.Y,
                        self.rotated_coords.Z)
                    debug_placer.place_symbol(
                        self.rotated_coords.X,
                        self.rotated_coords.Y,
                        self.rotated_coords.Z,
                        self.level_cylinder.z_max - self.level_cylinder.z_min,
                        comment
                    )
                break


class EquipmentDataCache:
    def __init__(self):
        self._cache = {}

    def collect_data(self, element, auditor_data):
        """Собирает данные в кэш. Запись в Revit произойдёт позже."""
        if element.Id not in self._cache:
            self._cache[element.Id] = {
                "element": element,
                "data": auditor_data,
                "setting": auditor_data.setting or None
            }
        else:
            # Если это клапан, и есть новая настройка — обновим
            if auditor_data.setting:
                self._cache[element.Id]["setting"] = auditor_data.setting

    def write_all(self):
        """Пишет все данные в Revit — один раз для каждого элемента"""
        for item in self._cache.values():
            element = item["element"]
            data = item["data"]
            setting = item["setting"]

            if data.type_name == EQUIPMENT_TYPE_NAME:
                real_power_watts = UnitConverter.to_watts(data.real_power)
                len_millimeters = UnitConverter.from_meters(data.equipment_len)
                element.SetParamValue('ADSK_Размер_Длина', len_millimeters)
                element.SetParamValue('ADSK_Код изделия', data.code)
                element.SetParamValue('ADSK_Тепловая мощность', real_power_watts)
            # В любом случае, если есть настройка — записываем

            if setting:
                element.SetParamValue('ADSK_Настройка', setting)


class ReadingRulesForEquipment:
    '''
    Класс используется для интерпретиции данных по Приборам
    '''
    connection_type_index = 2
    x_index = 3
    y_index = 4
    z_index = 5
    equipment_len_index = 12
    code_index = 16
    real_power_index = 20
    nominal_power_index = 22
    setting_index = 28
    maker_index = 30
    full_name_index = 31


class ReadingRulesForValve:
    '''
    Класс используется для интерпретиции данных по Клапанам
    '''
    connection_type_index = 1
    maker_index = 2
    x_index = 3
    y_index = 4
    z_index = 5
    setting_index = 17


class AuditorFileParser:
    """Отвечает только за парсинг строк файла в объекты"""
    @staticmethod
    def parse_heating_device(line, z_correction, angle):
        data = line.strip().split(';')
        rr = ReadingRulesForEquipment()
        # Получаем оба набора координат
        original_point, rotated_point = AuditorFileParser._parse_coordinates(
            data=data,
            x_idx=rr.x_index,
            y_idx=rr.y_index,
            z_idx=rr.z_index,
            z_correction=z_correction,
            angle=angle
        )
        return AuditorEquipment(
            connection_type=data[rr.connection_type_index],
            rotated_coords=rotated_point,
            original_coords=original_point,
            equipment_len=TextParser.parse_float(data[rr.equipment_len_index]),
            code=data[rr.code_index],
            real_power=TextParser.parse_float(data[rr.real_power_index]),
            nominal_power=TextParser.parse_float(data[rr.nominal_power_index]),
            setting=TextParser.parse_setting(data[rr.setting_index]),
            maker=data[rr.maker_index],
            full_name=data[rr.full_name_index],
            type_name=EQUIPMENT_TYPE_NAME
        )

    @staticmethod
    def parse_valve(line, z_correction, angle):
        data = line.strip().split(';')
        rr = ReadingRulesForValve()

        if data[rr.connection_type_index] != OUTER_VALVE_NAME:
            return None
        # Получаем оба набора координат
        original_point, rotated_point = AuditorFileParser._parse_coordinates(
            data=data,
            x_idx=rr.x_index,
            y_idx=rr.y_index,
            z_idx=rr.z_index,
            z_correction=z_correction,
            angle=angle
        )
        return AuditorEquipment(
            maker=data[rr.maker_index],
            rotated_coords=rotated_point,
            original_coords=original_point,
            setting=TextParser.parse_setting(data[rr.setting_index]),
            type_name=VALVE_TYPE_NAME
        )

    @staticmethod
    def _parse_coordinates(data, x_idx, y_idx, z_idx, z_correction, angle):
        """Парсит и возвращает как оригинальные, так и повернутые координаты"""
        x = UnitConverter.meters_to_millimeters(TextParser.parse_float(data[x_idx]))
        y = UnitConverter.meters_to_millimeters(TextParser.parse_float(data[y_idx]))
        z = UnitConverter.meters_to_millimeters(TextParser.parse_float(data[z_idx])) + z_correction

        # Получаем повернутые координаты
        original_point = XYZ(x, y, z)
        rotated_point = rotate_point(angle, original_point)

        # Возвращаем кортеж: (оригинальные_координаты, повернутые_координаты)
        return original_point, rotated_point


class ReportGenerator:
    @staticmethod
    def generate_area_overflow_report(auditor_equipment_list, revit_equipment_list):
        """
        Анализирует и возвращает данные о перекрытии областей оборудования
        Возвращает словарь {equipment_to_areas: [список координат областей]}
        """
        from collections import defaultdict

        equipment_to_areas = defaultdict(list)
        for auditor_equipment in auditor_equipment_list:
            equipment_in_area = [
                eq for eq in revit_equipment_list
                if auditor_equipment.is_in_data_area(eq)
            ]

            if len(equipment_in_area) > 1:
                for eq in equipment_in_area:
                    equipment_to_areas[eq.Id].append(auditor_equipment.original_coords)
        return equipment_to_areas

    @staticmethod
    def generate_not_found_report(auditor_equipment_list):
        """
        Анализирует и возвращает список необработанного оборудования
        Возвращает список объектов AuditorEquipment
        """
        return [
            equipment for equipment in auditor_equipment_list
            if not equipment.processed
        ]

    @staticmethod
    def print_area_overflow_report(overflow_data):
        """
        Выводит отчет о перекрытии областей
        """
        if not overflow_data:
            return
        print('Обнаружено переполнение данных областей:')

        for eq_id, areas in overflow_data.iteritems():
            print('ID элемента: {}'.format(eq_id))
            print('Элемент попал в области:')

            for coords in areas:
                print('  х: {}, y: {}, z: {}'.format(
                    coords[0],
                    coords[1],
                    coords[2]))
        print('\n')

    @staticmethod
    def print_not_found_report(not_found_equipment):
        """
        Выводит отчет о не найденном оборудовании
        """
        if not not_found_equipment:
            return
        print('\nНе найдено универсальное оборудование в областях:')

        for equipment in not_found_equipment:
            print('Прибор х: {}, y: {}, z: {}'.format(
                equipment.original_coords.X,
                equipment.original_coords.Y,
                equipment.original_coords.Z))


def calculate_z_correction(doc):
    internal_origin = InternalOrigin.Get(doc)
    base_point_z = BasePointHelper.get_base_point_z(doc)
    z_difference = base_point_z - internal_origin.SharedPosition.Z

    return UnitConverter.to_millimeters(z_difference)


def find_section(lines, title, start_offset, parse_func, z_correction, angle):
    result = []
    i = 0
    while i < len(lines):
        if title in lines[i]:
            i += start_offset

            while i < len(lines) and lines[i].strip():
                parsed_item = parse_func(lines[i], z_correction, angle)

                if parsed_item is not None:
                    result.append(parsed_item)
                i += 1
        i += 1
    return result


def get_bb_center(revit_bb):
    '''
    Получить центр Bounding Box
    '''
    minPoint = revit_bb.Min
    maxPoint = revit_bb.Max

    centroid = XYZ(
        (minPoint.X + maxPoint.X) / 2,
        (minPoint.Y + maxPoint.Y) / 2,
        (minPoint.Z + maxPoint.Z) / 2
    )
    return centroid


def rotate_point(angle, point):
    '''
    Поворот координат из исходных данных на указанный угол вокруг начала координат из Аудитора
    '''

    if angle == 0:
        return point

    # Угол в радианах
    angle_radians = math.radians(angle)
    # Матрица поворота вокруг оси Z (в плоскости XY)
    cos_theta = math.cos(angle_radians)
    sin_theta = math.sin(angle_radians)
    x_new = point.X * cos_theta - point.Y * sin_theta
    y_new = point.X * sin_theta + point.Y * cos_theta

    return XYZ(x_new, y_new, point.Z)


def get_elements_by_family_name(category):
    """ Возвращает коллекцию элементов по категории """
    revit_equipment_elements = FilteredElementCollector(doc) \
        .OfCategory(category) \
        .WhereElementIsNotElementType() \
        .ToElements()

    filtered_equipment = [
        eq for eq in revit_equipment_elements
        if FAMILY_NAME_CONST in eq.Symbol.Family.Name
    ]

    return filtered_equipment


def process_start_up():
    if doc.IsFamilyDocument:
        forms.alert("Надстройка не предназначена для работы с семействами", "Ошибка", exitscript=True)

    filepath = select_file('Файл расчетов (*.txt)|*.txt')

    if filepath is None:
        sys.exit()

    operator = JsonOperatorLib.JsonAngleOperator(doc, uiapp)

    # Получаем данные из последнего по дате редактирования файла
    old_angle = operator.get_json_data()

    angle = forms.ask_for_string(
        default=str(old_angle),
        prompt='Введите угол наклона модели в градусах:',
        title="Аудитор импорт"
    )

    try:
        angle = TextParser.parse_float(angle)
    except ValueError:
        forms.alert(
            "Необходимо ввести число.",
            "Ошибка",
            exitscript=True
        )

    if angle is None:
        sys.exit()

    operator.send_json_data(angle)
    return angle, filepath


def process_audytor_revit_matching(auditor_equipment_list, revit_equipment_list):
    data_cache = EquipmentDataCache()

    for ayditor_equipment in auditor_equipment_list:
        equipment_in_area = [
            eq for eq in revit_equipment_list if ayditor_equipment.is_in_data_area(eq)
        ]
        ayditor_equipment.processed = len(equipment_in_area) >= 1

        if len(equipment_in_area) == 1:
            data_cache.collect_data(equipment_in_area[0], ayditor_equipment)

    # Генерация и вывод отчетов
    overflow_data = ReportGenerator.generate_area_overflow_report(
        auditor_equipment_list,
        revit_equipment_list
    )
    ReportGenerator.print_area_overflow_report(overflow_data)
    not_found_equipment = ReportGenerator.generate_not_found_report(auditor_equipment_list)
    ReportGenerator.print_not_found_report(not_found_equipment)

    # Запись данных в Revit
    data_cache.write_all()


def read_auditor_file(file_path, angle, doc):
    with codecs.open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    z_correction = calculate_z_correction(doc)
    equipment = find_section(
        lines,
        "Отопительные приборы CO на плане",
        3,
        AuditorFileParser.parse_heating_device,
        z_correction,
        angle
    )
    valves = find_section(
        lines,
        "Арматура СО на плане",
        3,
        AuditorFileParser.parse_valve,
        z_correction,
        angle
    )
    equipment.extend(valves)

    if not equipment:
        forms.alert("Не найдено оборудование в импортируемом файле.", "Ошибка", exitscript=True)

    return equipment


if DEBUG_MODE:
    debug_placer = DebugPlacerLib.DebugPlacer(doc, diameter=2000)


@notification()
@log_plugin(EXEC_PARAMS.command_name)
def script_execute(plugin_logger):
    angle, filepath = process_start_up()
    ayditror_equipment_elements = read_auditor_file(filepath, angle, doc)
    # собираем высоты цилиндров в которых будем искать данные
    level_cylinders = LevelCylinderGenerator.create_cylinders(ayditror_equipment_elements)

    with revit.Transaction("BIM: Импорт приборов"):
        for ayditor_equipment in ayditror_equipment_elements:
            ayditor_equipment.set_level_cylinder(level_cylinders)

        equipment = get_elements_by_family_name(BuiltInCategory.OST_MechanicalEquipment)
        process_audytor_revit_matching(ayditror_equipment_elements, equipment)


script_execute()
