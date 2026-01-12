# -*- coding: utf-8 -*-
"""
Скрипт для чтения и анализа XML-отчётов Navisworks о пересечениях.

Показывает все проверки (включая пустые), фильтрует по текущей модели Revit,
печатает таблицы с подсветкой категорий и выводит сводку.
"""

__title__ = u"Пересечения"
__author__ = u"vlad / you"
__doc__ = u"Читает XML-отчёт Navisworks, показывает проверки, фильтрует по текущей модели Revit, выводит таблицы с подсветкой категории и сводку."

import os
import re
from collections import namedtuple
from datetime import datetime

from pyrevit import forms, script
from Autodesk.Revit.DB import ElementId


# =============================================================================
# КОНСТАНТЫ И НАСТРОЙКИ
# =============================================================================

# Фильтрация результатов
FILTER_BY_FILE = True       # Сверка файла из pathlink с текущей моделью
FILTER_BY_CATEGORY = False  # Дополнительная сверка категории в тексте pathlink

# Имена атрибутов для поиска ID объекта
OBJECT_ID_NAMES = frozenset([
    'объект id', 'object id', 'object 1 id',
    'object 2 id', 'id объекта'
])

# Атрибуты даты в XML
DATE_ATTRIBUTES = (
    'date', 'created', 'generated', 'exported', 'timestamp',
    'time', 'start', 'lastsaved', 'creationtime', 'modificationtime'
)

# CSS-стиль для подсветки категорий
BADGE_STYLE = u'background:#2f3b4a; color:#fff; padding:1px 6px; border-radius:3px; font-weight:600;'


# =============================================================================
# РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ
# =============================================================================

class Patterns:
    """Скомпилированные регулярные выражения для парсинга."""

    # Экранирование некорректных амперсандов в XML
    BAD_AMPERSAND = re.compile(
        u'&(?!amp;|lt;|gt;|apos;|quot;|#\\d+;|#x[0-9A-Fa-f]+;)',
        re.UNICODE
    )

    # Имя файла модели
    MODEL_FILE = re.compile(u'.+\\.(nwc|nwd|nwf|rvt)$', re.IGNORECASE)

    # Атрибут даты в XML-тексте
    DATE_ATTR = re.compile(
        r'\b(?:date|created|generated|export(?:date|ed)?|timestamp|time|'
        r'start|lastsaved|creationtime|modificationtime)\s*=\s*"([^"]+)"',
        re.IGNORECASE
    )

    # Fallback-парсер: блоки и атрибуты
    CLASHTEST_BLOCK = re.compile(r'<clashtest\b[^>]*>.*?</clashtest>', re.DOTALL | re.IGNORECASE)
    TEST_BLOCK = re.compile(r'<test\b[^>]*>.*?</test>', re.DOTALL | re.IGNORECASE)
    CLASHRESULT_BLOCK = re.compile(r'<clashresult\b[^>]*>.*?</clashresult>', re.DOTALL | re.IGNORECASE)
    CLASHOBJECT_BLOCK = re.compile(r'<clashobject\b[^>]*>.*?</clashobject>', re.DOTALL | re.IGNORECASE)

    ATTR_NAME = re.compile(r'\bname=\"([^\"]+)\"', re.IGNORECASE)
    ATTR_TESTNAME = re.compile(r'\btestname=\"([^\"]+)\"', re.IGNORECASE)
    ATTR_HREF = re.compile(r'\bhref=\"([^\"]*)\"', re.IGNORECASE)

    SMARTTAG_PAIR = re.compile(
        r'<smarttag>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</smarttag>',
        re.DOTALL | re.IGNORECASE
    )
    OBJECTATTR_PAIR = re.compile(
        r'<objectattribute>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</objectattribute>',
        re.DOTALL | re.IGNORECASE
    )
    PATHLINK_BLOCK = re.compile(r'<pathlink>.*?</pathlink>', re.DOTALL | re.IGNORECASE)
    NODE_TEXT = re.compile(r'<node>(.*?)</node>', re.DOTALL | re.IGNORECASE)
    HTML_TAGS = re.compile(r'<[^>]+>')


# =============================================================================
# СТРУКТУРЫ ДАННЫХ
# =============================================================================

ClashObject = namedtuple('ClashObject', ['element_id', 'path'])
ClashRow = namedtuple('ClashRow', [
    'name', 'image_path', 'element_id', 'other_element_id',
    'path', 'other_path', 'category', 'other_category'
])


# =============================================================================
# ИНИЦИАЛИЗАЦИЯ REVIT
# =============================================================================

out = script.get_output()
out.close_others(all_open_outputs=True)
out.set_width(1200)

uidoc = __revit__.ActiveUIDocument
doc = uidoc.Document


# =============================================================================
# КЭШ ЭЛЕМЕНТОВ И КАТЕГОРИЙ
# =============================================================================

class ElementCache:
    """Кэш для элементов Revit и их категорий."""

    def __init__(self, document):
        self._doc = document
        self._cache = {}
        self._category_keys = self._build_category_keys()
        self._doc_title_key = self._normalize_key(document.Title)

    def _normalize_key(self, text):
        """Нормализует строку для сравнения."""
        if not text:
            return u""
        return re.sub(u'[^0-9a-zA-Zа-яА-Я]+', u'', text, flags=re.UNICODE).lower()

    def _build_category_keys(self):
        """Собирает нормализованные имена категорий документа."""
        keys = set()
        try:
            for cat in self._doc.Settings.Categories:
                try:
                    if cat.Name:
                        keys.add(self._normalize_key(cat.Name))
                except Exception:
                    pass
        except Exception:
            pass
        return keys

    def get_element_and_category(self, element_id):
        """Возвращает (element, category_name) для заданного ID."""
        if element_id in self._cache:
            return self._cache[element_id]

        try:
            element = self._doc.GetElement(ElementId(element_id))
            category = u""
            if element and element.Category:
                category = element.Category.Name or u""
        except Exception:
            element, category = None, u""

        self._cache[element_id] = (element, category)
        return element, category

    def matches_current_model(self, path_text):
        """Проверяет, относится ли путь к текущей модели."""
        filename = self._extract_filename(path_text)
        if not filename:
            return True

        filename_key = self._normalize_key(filename)
        return filename_key in self._doc_title_key or self._doc_title_key in filename_key

    def _extract_filename(self, path_text):
        """Извлекает имя файла модели из пути."""
        if not path_text:
            return u''

        for token in path_text.split(u'/'):
            token = token.strip()
            if Patterns.MODEL_FILE.match(token):
                return os.path.splitext(token)[0]
        return u''

    def find_category_in_path(self, segment_normalized):
        """Ищет категорию документа в сегменте пути."""
        if not segment_normalized or len(segment_normalized) < 4:
            return None

        for cat_key in self._category_keys:
            if not cat_key or len(cat_key) < 4:
                continue
            if cat_key in segment_normalized or segment_normalized in cat_key:
                return cat_key
        return None

    @property
    def doc_title_key(self):
        return self._doc_title_key

    def normalize_key(self, text):
        return self._normalize_key(text)


# Глобальный экземпляр кэша
element_cache = ElementCache(doc)


# =============================================================================
# ЧТЕНИЕ И ОЧИСТКА XML
# =============================================================================

class XmlReader:
    """Класс для безопасного чтения XML-файлов."""

    ENCODINGS = ('utf-8-sig', 'utf-16', 'utf-8', 'cp1251')

    @classmethod
    def read_file(cls, path):
        """Читает файл с автоопределением кодировки."""
        with open(path, 'rb') as f:
            data = f.read()

        for encoding in cls.ENCODINGS:
            try:
                return data.decode(encoding)
            except Exception:
                pass
        return data.decode('utf-8', 'ignore')

    @classmethod
    def sanitize(cls, text):
        """Очищает текст от некорректных XML-символов."""
        # Нормализация переносов строк
        text = text.replace(u'\r\n', u'\n').replace(u'\r', u'\n')
        text = text.replace(u'\ufeff', u' ')

        # Удаление недопустимых символов
        text = cls._strip_invalid_chars(text)

        # Экранирование амперсандов
        text = Patterns.BAD_AMPERSAND.sub(u'&amp;', text)

        # Обрезка до начала XML
        start = text.find(u'<')
        if start > 0:
            text = text[start:]

        return text

    @staticmethod
    def _strip_invalid_chars(text):
        """Удаляет недопустимые XML-символы."""
        result = []
        for char in text:
            code = ord(char)
            if (code == 0x9 or code == 0xA or code == 0xD or
                (0x20 <= code <= 0xD7FF) or
                (0xE000 <= code <= 0xFFFD) or
                (0x10000 <= code <= 0x10FFFF)):
                result.append(char)
            else:
                result.append(u' ')
        return u''.join(result)

    @classmethod
    def parse(cls, xml_path):
        """Парсит XML-файл и возвращает корневой элемент."""
        try:
            from lxml import etree as ET
        except ImportError:
            import xml.etree.ElementTree as ET

        raw_text = cls.read_file(xml_path)
        cleaned_text = cls.sanitize(raw_text)
        return ET.fromstring(cleaned_text)


# =============================================================================
# ПАРСИНГ XML-ОТЧЁТА
# =============================================================================

class NavisworksReportParser:
    """Парсер XML-отчётов Navisworks."""

    def __init__(self, xml_path):
        self.xml_path = xml_path
        self.base_dir = os.path.dirname(xml_path)

    def parse(self):
        """Парсит отчёт и возвращает dict: test_name -> [ClashRow, ...]."""
        try:
            return self._parse_with_xml_parser()
        except Exception as e:
            out.print_md(u":warning: XML-парсер не справился (`{}`). Использую fallback...".format(e))
            return self._parse_with_regex()

    def _parse_with_xml_parser(self):
        """Основной парсер на базе XML."""
        root = XmlReader.parse(self.xml_path)

        # Собираем имена всех тестов
        test_names = self._collect_test_names(root)
        groups = {name: [] for name in test_names}

        # Строим карту родительских элементов
        parent_map = self._build_parent_map(root)

        # Парсим результаты
        for clash_result in root.findall('.//clashresult'):
            self._process_clash_result(clash_result, groups, parent_map)

        return groups

    def _collect_test_names(self, root):
        """Собирает имена всех тестов."""
        names = set()
        for tag in ('clashtest', 'test'):
            for element in root.findall('.//{}'.format(tag)):
                name = element.get('name') or element.get('displayname')
                if name:
                    names.add(name)
        return names

    def _build_parent_map(self, root):
        """Строит карту ребёнок -> родитель."""
        parent_map = {}
        for parent in root.iter():
            for child in list(parent):
                parent_map[child] = parent
        return parent_map

    def _process_clash_result(self, clash_result, groups, parent_map):
        """Обрабатывает один clashresult."""
        test_name = self._get_test_name(clash_result, parent_map)
        clash_name = clash_result.get('name') or u'Без имени'
        image_path = self._resolve_image_path(clash_result.get('href'))

        objects = []
        for clash_object in clash_result.findall('./clashobjects/clashobject'):
            obj = self._parse_clash_object(clash_object)
            if obj:
                objects.append(obj)

        if not objects:
            return

        rows = groups.setdefault(test_name, [])
        self._add_rows(rows, clash_name, image_path, objects)

    def _get_test_name(self, clash_result, parent_map):
        """Определяет имя теста для clashresult."""
        # Сначала ищем в атрибутах самого элемента
        for attr in ('testname', 'test', 'groupname'):
            name = clash_result.get(attr)
            if name:
                return name

        # Ищем в родительских элементах
        parent = parent_map.get(clash_result)
        while parent is not None:
            name = parent.get('name') or parent.get('displayname') or parent.get('testname')
            if name:
                return name
            parent = parent_map.get(parent)

        return u'(Без названия проверки)'

    def _parse_clash_object(self, element):
        """Парсит clashobject и возвращает ClashObject."""
        element_id = self._extract_object_id(element)
        if element_id is None:
            return None

        path = self._extract_pathlink(element)
        return ClashObject(element_id=element_id, path=path)

    def _extract_object_id(self, element):
        """Извлекает ID объекта из smarttags или objectattribute."""
        # Ищем в smarttags
        for smarttag in element.findall('./smarttags/smarttag'):
            name = (smarttag.findtext('name') or '').strip().lower()
            if name in OBJECT_ID_NAMES:
                value = (smarttag.findtext('value') or '').strip()
                if value.isdigit():
                    return int(value)

        # Ищем в objectattribute
        for obj_attr in element.findall('./objectattribute'):
            name = (obj_attr.findtext('name') or '').strip().lower()
            if name in OBJECT_ID_NAMES:
                value = (obj_attr.findtext('value') or '').strip()
                if value.isdigit():
                    return int(value)

        return None

    def _extract_pathlink(self, element):
        """Извлекает путь из pathlink."""
        parts = []

        # Пробуем node
        for node in element.findall('./pathlink/node'):
            text = (node.text or '').strip()
            if text:
                parts.append(text)

        # Если не нашли, пробуем path
        if not parts:
            for path in element.findall('./pathlink/path'):
                text = (path.text or '').strip()
                if text:
                    parts.append(text)

        return u' / '.join(parts)

    def _resolve_image_path(self, href):
        """Преобразует относительный путь к изображению в абсолютный."""
        if not href:
            return u''
        href = href.replace('\\', '/').lstrip('./')
        return os.path.normpath(os.path.join(self.base_dir, href))

    def _add_rows(self, rows, clash_name, image_path, objects):
        """Добавляет строки для пары объектов."""
        if len(objects) >= 2:
            obj_a, obj_b = objects[0], objects[1]
            # Добавляем обе перестановки
            rows.append({
                'name': clash_name, 'img': image_path,
                'id': obj_a.element_id, 'id_other': obj_b.element_id,
                'path': obj_a.path, 'path_other': obj_b.path
            })
            rows.append({
                'name': clash_name, 'img': image_path,
                'id': obj_b.element_id, 'id_other': obj_a.element_id,
                'path': obj_b.path, 'path_other': obj_a.path
            })
        else:
            obj = objects[0]
            rows.append({
                'name': clash_name, 'img': image_path,
                'id': obj.element_id, 'id_other': None,
                'path': obj.path, 'path_other': u''
            })

    def _parse_with_regex(self):
        """Fallback-парсер на базе регулярных выражений."""
        text = XmlReader.sanitize(XmlReader.read_file(self.xml_path))
        groups = {}

        # Ищем блоки тестов
        blocks = list(Patterns.CLASHTEST_BLOCK.finditer(text))
        if not blocks:
            blocks = list(Patterns.TEST_BLOCK.finditer(text))
        if not blocks:
            # Обрабатываем весь текст как один блок
            blocks = [type('FakeMatch', (), {'group': lambda self, n=0: text})()]

        for block_match in blocks:
            block_text = block_match.group(0) if hasattr(block_match, 'group') else str(block_match)

            # Определяем имя теста
            name_match = Patterns.ATTR_NAME.search(block_text)
            default_test_name = name_match.group(1) if name_match else u'(Без названия проверки)'
            groups.setdefault(default_test_name, [])

            # Парсим результаты
            for result_match in Patterns.CLASHRESULT_BLOCK.finditer(block_text):
                self._process_regex_result(result_match.group(0), groups, default_test_name)

        return groups

    def _process_regex_result(self, result_text, groups, default_test_name):
        """Обрабатывает clashresult через regex."""
        # Имя теста
        testname_match = Patterns.ATTR_TESTNAME.search(result_text)
        test_name = testname_match.group(1) if testname_match else default_test_name

        # Путь к изображению
        href_match = Patterns.ATTR_HREF.search(result_text)
        image_path = self._resolve_image_path(href_match.group(1) if href_match else u'')

        # Имя коллизии
        name_match = Patterns.ATTR_NAME.search(result_text)
        clash_name = name_match.group(1) if name_match else u'Без имени'

        # Парсим объекты
        objects = []
        for obj_match in Patterns.CLASHOBJECT_BLOCK.finditer(result_text):
            obj = self._parse_regex_object(obj_match.group(0))
            if obj:
                objects.append(obj)

        if not objects:
            return

        rows = groups.setdefault(test_name, [])
        self._add_rows(rows, clash_name, image_path, objects)

    def _parse_regex_object(self, obj_text):
        """Парсит clashobject через regex."""
        element_id = None

        # Ищем в smarttags
        for name, value in Patterns.SMARTTAG_PAIR.findall(obj_text):
            if (name or u'').strip().lower() in OBJECT_ID_NAMES:
                v = (value or u'').strip()
                if v.isdigit():
                    element_id = int(v)
                    break

        # Ищем в objectattribute
        if element_id is None:
            for name, value in Patterns.OBJECTATTR_PAIR.findall(obj_text):
                if (name or u'').strip().lower() in OBJECT_ID_NAMES:
                    v = (value or u'').strip()
                    if v.isdigit():
                        element_id = int(v)
                        break

        if element_id is None:
            return None

        # Извлекаем путь
        path = u''
        pathlink_match = Patterns.PATHLINK_BLOCK.search(obj_text)
        if pathlink_match:
            nodes = Patterns.NODE_TEXT.findall(pathlink_match.group(0))
            parts = [Patterns.HTML_TAGS.sub(u'', n).strip() for n in nodes if n.strip()]
            path = u' / '.join(parts)

        return ClashObject(element_id=element_id, path=path)


# =============================================================================
# ИЗВЛЕЧЕНИЕ ДАТЫ ОТЧЁТА
# =============================================================================

def extract_report_datetime(xml_path):
    """Извлекает дату отчёта из XML или метаданных файла."""
    # Пробуем из XML-атрибутов
    try:
        root = XmlReader.parse(xml_path)

        # Ищем в корневом элементе
        for attr in DATE_ATTRIBUTES:
            value = root.get(attr)
            if value:
                return value, u"из XML"

        # Ищем в дочерних элементах
        for tag in ('report', 'batchtest', 'tests'):
            for element in root.findall('.//{}'.format(tag)):
                for attr in DATE_ATTRIBUTES:
                    value = element.get(attr)
                    if value:
                        return value, u"из XML"
    except Exception:
        pass

    # Пробуем regex-поиск
    try:
        text = XmlReader.sanitize(XmlReader.read_file(xml_path))
        match = Patterns.DATE_ATTR.search(text)
        if match:
            return match.group(1).strip(), u"из XML"
    except Exception:
        pass

    # Берём дату изменения файла
    try:
        timestamp = os.path.getmtime(xml_path)
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime(u"%d.%m.%Y %H:%M"), u"по дате изменения файла"
    except Exception:
        return u"", u""


# =============================================================================
# ФИЛЬТРАЦИЯ И АННОТАЦИЯ
# =============================================================================

class ResultFilter:
    """Фильтрация и аннотация результатов."""

    def __init__(self, cache):
        self.cache = cache

    def filter_and_annotate(self, items):
        """Фильтрует строки и добавляет информацию о категориях."""
        result = []

        for item in items:
            annotated = self._process_item(item)
            if annotated:
                result.append(annotated)

        return result

    def _process_item(self, item):
        """Обрабатывает одну строку."""
        try:
            element_id = int(item['id'])
            element, category = self.cache.get_element_and_category(element_id)

            if not element:
                return None

            # Фильтр по файлу
            if FILTER_BY_FILE:
                path_matches = (
                    self.cache.matches_current_model(item.get('path', u'')) or
                    self.cache.matches_current_model(item.get('path_other', u''))
                )
                if not path_matches:
                    return None

            # Фильтр по категории
            if FILTER_BY_CATEGORY and category:
                cat_lower = category.strip().lower()
                path1 = (item.get('path', u'') or u'').lower()
                path2 = (item.get('path_other', u'') or u'').lower()
                if cat_lower not in path1 and cat_lower not in path2:
                    return None

            # Создаём аннотированную копию
            result = dict(item)
            result['cat'] = category

            # Категория второго элемента
            other_id = item.get('id_other')
            if other_id:
                other_element, other_category = self.cache.get_element_and_category(int(other_id))
                result['cat_other'] = other_category if other_element else u""
            else:
                result['cat_other'] = u""

            return result

        except Exception:
            return None


# =============================================================================
# ПОДСВЕТКА КАТЕГОРИЙ В ПУТИ
# =============================================================================

class PathHighlighter:
    """Подсветка категорий в путях элементов."""

    def __init__(self, cache):
        self.cache = cache

    def highlight(self, path_text, category_name):
        """Подсвечивает категорию в пути."""
        if not path_text:
            return u'—'

        segments = [s.strip() for s in path_text.split(u'/')]
        highlighted = False

        # 1. Пробуем подсветить по имени категории
        if category_name:
            category_key = self.cache.normalize_key(category_name.strip())
            if category_key:
                for i, segment in enumerate(segments):
                    if category_key in self.cache.normalize_key(segment):
                        segments[i] = self._wrap_in_badge(segment)
                        highlighted = True
                        break

        # 2. Если не нашли - ищем по списку категорий документа
        if not highlighted:
            start_index = self._find_start_after_filename(segments)
            for i in range(start_index, len(segments)):
                if '<span' in segments[i]:
                    continue

                segment_key = self.cache.normalize_key(segments[i])
                if self.cache.find_category_in_path(segment_key):
                    segments[i] = self._wrap_in_badge(segments[i])
                    break

        return u' / '.join(segments)

    def _find_start_after_filename(self, segments):
        """Находит индекс сегмента после имени файла."""
        for i, segment in enumerate(segments):
            if Patterns.MODEL_FILE.match(segment):
                return i + 1
        return 0

    def _wrap_in_badge(self, text):
        """Оборачивает текст в span с подсветкой."""
        return u'<span style="{style}">{text}</span>'.format(style=BADGE_STYLE, text=text)


# =============================================================================
# СТАТИСТИКА
# =============================================================================

class StatisticsBuilder:
    """Построение статистики по коллизиям."""

    def __init__(self, highlighter):
        self.highlighter = highlighter

    def build(self, filtered_groups):
        """Строит статистику по группам."""
        stats = {}

        for test_name, rows in filtered_groups.items():
            seen_pairs = set()
            category_pairs = set()

            for item in rows:
                pair = self._extract_pair(item)
                if not pair or pair in seen_pairs:
                    continue

                seen_pairs.add(pair)
                cat_pair = self._extract_category_pair(item)
                if cat_pair:
                    category_pairs.add(cat_pair)

            stats[test_name] = {
                'pairs': len(seen_pairs),
                'catpairs': category_pairs
            }

        return stats

    def _extract_pair(self, item):
        """Извлекает пару ID элементов."""
        id1 = item.get('id')
        id2 = item.get('id_other')

        if not id1 or not id2:
            return None

        try:
            a, b = int(id1), int(id2)
            return (a, b) if a < b else (b, a)
        except Exception:
            return None

    def _extract_category_pair(self, item):
        """Извлекает пару категорий."""
        cat1 = (item.get('cat') or u"").strip()
        cat2 = (item.get('cat_other') or u"").strip()

        # Если вторая категория не определена - пробуем извлечь из пути
        if not cat2:
            highlighted = self.highlighter.highlight(item.get('path_other') or u'', u'')
            # Извлекаем текст из span
            match = re.search(u'<span[^>]*>([^<]+)</span>', highlighted)
            if match:
                cat2 = match.group(1)
            else:
                cat2 = re.sub(u'<[^>]*>', u'', highlighted)

        if cat1 or cat2:
            return tuple(sorted([cat1 or u'—', cat2 or u'—']))
        return None


# =============================================================================
# ВЫВОД РЕЗУЛЬТАТОВ
# =============================================================================

class ResultPrinter:
    """Вывод результатов в UI."""

    def __init__(self, output, highlighter):
        self.out = output
        self.highlighter = highlighter

    def print_report_date(self, date_value, date_source):
        """Печатает дату отчёта."""
        if date_value:
            self.out.print_md(u"**Дата отчёта Navisworks:** {} _({})_".format(
                date_value, date_source
            ))

    def print_summary(self, filtered_groups, stats):
        """Печатает краткую сводку."""
        self.out.print_md(u"### Краткая сводка по выбранным проверкам")

        lines = []
        for test_name in sorted(filtered_groups.keys(), key=lambda s: s.lower()):
            st = stats.get(test_name, {'pairs': 0, 'catpairs': set()})
            cat_pairs_str = u'; '.join(
                u'{} × {}'.format(a, b) for (a, b) in sorted(st['catpairs'])
            )
            lines.append(u"- **{}** — {} коллизий; пары категорий: {}".format(
                test_name, st['pairs'], cat_pairs_str or u'—'
            ))

        self.out.print_md(u'\n'.join(lines))

    def print_total(self, filtered_groups):
        """Печатает общее количество."""
        total_rows = sum(len(rows) for rows in filtered_groups.values())
        self.out.print_md(u"## Пересечения: {} строк ({} проверок)".format(
            total_rows, len(filtered_groups)
        ))

    def print_group(self, title, rows):
        """Печатает таблицу для группы проверок."""
        self.out.print_md(u"\n---\n### Проверка: **{}**  _(строк: {})_".format(
            title, len(rows)
        ))

        if not rows:
            self.out.print_md(u"—")
            return

        table_data = []
        for i, item in enumerate(rows, 1):
            path1 = self.highlighter.highlight(item.get('path') or u'', item.get('cat') or u'')
            path2 = self.highlighter.highlight(item.get('path_other') or u'', item.get('cat_other') or u'')

            table_data.append([
                i,
                self._format_image(item.get('img')),
                title,
                item.get('name') or u'',
                self.out.linkify(ElementId(int(item['id']))),
                path1 or u'—',
                path2 or u'—'
            ])

        self.out.print_table(
            table_data=table_data,
            columns=[
                u'№', u'Снимок', u'Название проверки', u'Пересечение',
                u'ID', u'Путь элемента', u'Путь второго элемента'
            ],
            title=None
        )

    def print_footer(self):
        """Печатает подвал."""
        self.out.print_md(u"_Клик по **ID** выделяет элемент. Можно кликать подряд._")

    def _format_image(self, path, width=96):
        """Форматирует ячейку с изображением."""
        if not path or not os.path.exists(path):
            return u'—'
        uri = u'file:///' + path.replace('\\', '/')
        return u'<img src="{}" width="{}" />'.format(uri, int(width))


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    # Выбор файла
    xml_path = forms.pick_file(
        files_filter="XML (*.xml)|*.xml",
        title=u"Выберите XML отчёт Navisworks"
    )
    if not xml_path:
        forms.alert(u"Файл не выбран.", title=u"Пересечения")
        return

    # Парсинг
    parser = NavisworksReportParser(xml_path)
    groups = parser.parse()

    if not groups:
        forms.alert(u"Не удалось извлечь данные из отчёта.", title=u"Пересечения")
        return

    # Выбор проверок
    all_tests = sorted(groups.keys(), key=lambda s: s.lower())
    picked = forms.SelectFromList.show(
        all_tests,
        multiselect=True,
        title=u"Выберите проверки (можно несколько). Вверху есть строка поиска.",
        button_name=u"Показать"
    )

    if picked and len(picked) < len(all_tests):
        groups = {t: groups.get(t, []) for t in picked}

    # Инициализация компонентов
    highlighter = PathHighlighter(element_cache)
    result_filter = ResultFilter(element_cache)
    stats_builder = StatisticsBuilder(highlighter)
    printer = ResultPrinter(out, highlighter)

    # Дата отчёта
    report_date, date_source = extract_report_datetime(xml_path)
    printer.print_report_date(report_date, date_source)

    # Фильтрация
    filtered_groups = {
        test: result_filter.filter_and_annotate(rows)
        for test, rows in groups.items()
    }

    # Статистика и вывод
    stats = stats_builder.build(filtered_groups)
    printer.print_summary(filtered_groups, stats)
    printer.print_total(filtered_groups)

    for test_name in sorted(filtered_groups.keys(), key=lambda s: s.lower()):
        printer.print_group(test_name, filtered_groups[test_name])

    printer.print_footer()


if __name__ == "__main__":
    main()
