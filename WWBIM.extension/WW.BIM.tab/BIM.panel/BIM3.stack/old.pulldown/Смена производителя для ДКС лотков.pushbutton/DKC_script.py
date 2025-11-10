# -*- coding: utf-8 -*-
# pyRevit (IronPython 2.7) — Excel/CSV → ADSK для лотков/фитингов/прогонов и механического оборудования
# - Читает Excel/CSV надёжно (COM, Protected View, перебор всех листов, System.Array 2-D)
# - Заполняет: ADSK_Наименование, ADSK_Код изделия, ADSK_Завод-изготовитель
# - Копирует:  DKC_Единица изменения → ADSK_Единица изменения
# - Количество: DKC_ДлинаФакт (в м) → ADSK_Количество; если нет — DKC_Количество
# - Пишет на экземпляр, если нельзя — на тип.
# - Дополнительно: печатает в окне вывода pyRevit сводку и таблицы
#   по элементам, для которых не найдено соответствие в исходной таблице
#   (кликабельные ID для выделения элементов).
# - Таблица «не найдено» ДЕДУПЛИЦИРУЕТСЯ по уникальному DKC_Код изделия (1 строка на код).
__title__  = u"Смена производителя\nдля DKC лотков"
__author__ = "Влад"
__doc__    = u"""На основе файла исходных данных заполняет ADSK параметры для лотков и соед. деталей.
Выводит таблицу только с ОДНИМ представителем на каждый уникальный DKC_Код изделия, если код не найден в исходной таблице."""

import clr, os, re
clr.AddReference('RevitAPI')
from Autodesk.Revit.DB import *

# ===== Revit doc =====
try:
    from pyrevit import revit
    doc = revit.doc
except:
    clr.AddReference('RevitServices')
    from RevitServices.Persistence import DocumentManager
    doc = DocumentManager.Instance.CurrentDBDocument

# ===== pyRevit Output (для кликабельных ID и таблиц) =====
_HAS_OUTPUT = False
out = None
try:
    from pyrevit import script as _pyscript
    out = _pyscript.get_output()
    out.close_others(all_open_outputs=True)
    out.set_width(1200)
    _HAS_OUTPUT = True
except Exception:
    pass

# ===== WinForms диалоги =====
clr.AddReference("System")
clr.AddReference("System.Drawing")
clr.AddReference("System.Windows.Forms")
from System.Windows.Forms import OpenFileDialog, MessageBox, DialogResult

# ------------ Параметры в модели ------------
SRC_CODE_CANDIDATES   = [u"DKC_Код изделия", u"DKC_Код Изделия"]
SRC_LENFACT           = u"DKC_ДлинаФакт"
SRC_QTY               = u"DKC_Количество"
SRC_UNIT              = u"DKC_Единица изменения"   # → ADSK_Единица изменения

DST_NAME              = u"ADSK_Наименование"
DST_CODE              = u"ADSK_Код изделия"
DST_MFR               = u"ADSK_Завод-изготовитель"
DST_QTY               = u"ADSK_Количество"
DST_UNIT              = u"ADSK_Единица изменения"

# Категории
CATS = [
    BuiltInCategory.OST_CableTray,
    BuiltInCategory.OST_CableTrayFitting,
    BuiltInCategory.OST_MechanicalEquipment
]
try:
    CATS.append(BuiltInCategory.OST_CableTrayRun)   # если версия поддерживает
except:
    pass

# ------------ Утилиты Revit ------------
try:
    _UNIT_ID_METERS = UnitTypeId.Meters
except:
    _UNIT_ID_METERS = None

def feet_to_meters(val_feet):
    try:
        if _UNIT_ID_METERS is not None:
            return UnitUtils.ConvertFromInternalUnits(val_feet, _UNIT_ID_METERS)
        else:
            return UnitUtils.ConvertFromInternalUnits(val_feet, DisplayUnitType.DUT_METERS)
    except:
        try:
            return UnitUtils.ConvertFromInternalUnits(val_feet, DisplayUnitType.DUT_METERS)
        except:
            return None

def parse_length_string_to_meters(s):
    if not s: return None
    t = (u"%s" % s).strip().lower().replace(' ', '')
    m = re.match(u'^([+-]?[0-9]+(?:[\.,][0-9]+)?)(.*)$', t)
    if not m: return None
    num = float(m.group(1).replace(',', '.'))
    unit = m.group(2)
    if unit.startswith(u'мм') or unit == u'mm': return num / 1000.0
    if unit.startswith(u'см') or unit == u'cm': return num / 100.0
    if unit.startswith(u'м')  or unit == u'm':  return num
    if unit.startswith('ft') or (u'′' in unit) or unit == u'фут': return num * 0.3048
    if unit.startswith('in') or (u'″' in unit) or unit == u'дюйм' or unit == u'"': return num * 0.0254
    return None

def get_type_element(el):
    try:
        if hasattr(el, "Symbol") and el.Symbol:
            return el.Symbol
    except:
        pass
    try:
        tid = el.GetTypeId()
        if tid and tid != ElementId.InvalidElementId:
            return doc.GetElement(tid)
    except:
        pass
    return None

def lookup_param(target, name):
    if not target: return None
    try:
        return target.LookupParameter(name)
    except:
        return None

def get_val_and_type(p):
    if not p: return (None, None)
    st = p.StorageType
    try:
        if st == StorageType.String:
            s = p.AsString()
            return ((s.strip() if s else None), st)
        if st == StorageType.Double:
            return (p.AsDouble(), st)
        if st == StorageType.Integer:
            return (p.AsInteger(), st)
        if st == StorageType.ElementId:
            return (p.AsElementId(), st)
    except:
        pass
    return (None, None)

def is_empty_value(value, st):
    if value is None: return True
    if st == StorageType.String:  return (u"%s" % value).strip() == u""
    if st == StorageType.Double:
        try: return abs(float(value)) < 1e-9
        except: return True
    if st == StorageType.Integer:
        try: return int(value) == 0
        except: return True
    return False

def set_val_typed(p, value, src_st=None):
    if not p or p.IsReadOnly or value is None: return False
    dst = p.StorageType
    try:
        if dst == StorageType.String:
            return p.Set(u"%s" % value)
        if dst == StorageType.Integer:
            if src_st == StorageType.Integer:
                return p.Set(int(value))
            if src_st in (StorageType.Double, StorageType.String):
                try:
                    return p.Set(int(round(float((u"%s" % value).replace(',', '.')))))
                except:
                    return False
        if dst == StorageType.Double:
            if src_st == StorageType.Double:
                return p.Set(float(value))
            if src_st in (StorageType.Integer, StorageType.String):
                try:
                    return p.Set(float((u"%s" % value).replace(',', '.')))
                except:
                    return False
        if dst == StorageType.ElementId and src_st == StorageType.ElementId and isinstance(value, ElementId):
            return p.Set(value)
        return False
    except:
        return False

def set_on_inst_or_type(el, pname, value, src_st=None):
    # сначала пробуем экземпляр, затем — тип
    if set_val_typed(lookup_param(el, pname), value, src_st):
        return True
    return set_val_typed(lookup_param(get_type_element(el), pname), value, src_st)

def collect_elements(categories):
    ids = set(); result = []
    for bic in categories:
        try:
            col = FilteredElementCollector(doc).OfCategory(bic).WhereElementIsNotElementType()
            for el in col:
                if el and (el.Id.IntegerValue not in ids):
                    ids.add(el.Id.IntegerValue)
                    result.append(el)
        except:
            pass
    return result

def family_type_string(el, typ):
    fam = u""
    tname = u""
    try:
        if typ:
            try:
                fam = typ.FamilyName
            except:
                fam = u""
            try:
                if not fam and hasattr(typ, "Family") and getattr(typ, "Family"):
                    fam = typ.Family.Name
            except:
                pass
            try:
                tname = typ.get_Parameter(BuiltInParameter.SYMBOL_NAME_PARAM).AsString() or typ.Name
            except:
                tname = getattr(typ, "Name", u"")
    except:
        pass
    if fam and tname:
        return u"%s : %s" % (fam, tname)
    return tname or fam or u""

# ------------ Надёжный Excel/CSV reader (COM) ------------
import System
import System.Runtime.InteropServices as interop
clr.AddReference('Microsoft.VisualBasic')
from Microsoft.VisualBasic.FileIO import TextFieldParser, FieldType
from System import Type, Activator

def _cleanup_com(*objs):
    for o in objs:
        try:
            if o is not None:
                interop.Marshal.FinalReleaseComObject(o)
        except:
            pass

def _to_rows_2d(value2):
    """Excel Range.Value2 -> list[list[str]]; поддержка System.Array 2-D и tuple 2-D."""
    rows = []
    if value2 is None:
        return rows

    # System.Array 2-D
    try:
        if isinstance(value2, System.Array) and value2.Rank == 2:
            r0 = value2.GetLowerBound(0); r1 = value2.GetUpperBound(0)
            c0 = value2.GetLowerBound(1); c1 = value2.GetUpperBound(1)
            for ri in range(r0, r1 + 1):
                row = []
                for ci in range(c0, c1 + 1):
                    cell = value2.GetValue(ri, ci)
                    row.append(u"" if cell is None else (unicode(cell) if not isinstance(cell, unicode) else cell))
                while len(row) < 5:
                    row.append(u"")
                rows.append(row[:5])
            return rows
    except:
        pass

    # tuple-of-tuples
    if isinstance(value2, tuple):
        for r in value2:
            if isinstance(r, tuple):
                row = [u"" if c is None else (unicode(c) if not isinstance(c, unicode) else c) for c in r]
            else:
                row = [u"" if r is None else unicode(r)]
            while len(row) < 5:
                row.append(u"")
            rows.append(row[:5])
        return rows

    # scalar
    row = [u"" if value2 is None else unicode(value2)]
    while len(row) < 5:
        row.append(u"")
    return [row[:5]]

def _nonempty_cell_count(value2):
    """Счётчик непустых ячеек для выбора лучшего листа."""
    if value2 is None:
        return 0
    try:
        if isinstance(value2, System.Array) and value2.Rank == 2:
            r0 = value2.GetLowerBound(0); r1 = value2.GetUpperBound(0)
            c0 = value2.GetLowerBound(1); c1 = value2.GetUpperBound(1)
            cnt = 0
            for ri in range(r0, r1 + 1):
                for ci in range(c0, c1 + 1):
                    v = value2.GetValue(ri, ci)
                    if v not in (None, u""):
                        cnt += 1
            return cnt
    except:
        pass
    if isinstance(value2, tuple):
        cnt = 0
        for r in value2:
            if isinstance(r, tuple):
                for c in r:
                    if c not in (None, u""):
                        cnt += 1
            else:
                if r not in (None, u""):
                    cnt += 1
        return cnt
    return 0 if value2 in (None, u"") else 1

def read_csv_fast(path):
    """CSV без Excel (поддержка ; и , , кавычек)."""
    rows = []
    p = TextFieldParser(path)
    p.TextFieldType = FieldType.Delimited
    p.SetDelimiters(";", ",")
    p.HasFieldsEnclosedInQuotes = True
    try:
        while not p.EndOfData:
            fields = list(p.ReadFields() or [])
            while len(fields) < 5:
                fields.append(u"")
            rows.append([u"" if f is None else (unicode(f) if not isinstance(f, unicode) else f) for f in fields[:5]])
    finally:
        p.Close()
    return rows

def read_excel_via_com(path, sheet_name=None):
    """XLS/XLSX через Excel COM; Protected View обходится через ProtectedViewWindows.Open(...).Edit()."""
    excel = wb = None
    rows = []
    excel_type = Type.GetTypeFromProgID("Excel.Application")
    if excel_type is None:
        raise Exception(u"Excel не установлен (ProgID Excel.Application недоступен)")

    try:
        excel = Activator.CreateInstance(excel_type)
        excel.Visible = False
        excel.DisplayAlerts = False

        # Обычное открытие
        try:
            wb = excel.Workbooks.Open(path, ReadOnly=True, IgnoreReadOnlyRecommended=True)
        except interop.COMException:
            # Protected View → открываем и переводим в edit
            pv = None
            try:
                pv = excel.ProtectedViewWindows.Open(path)
                try:
                    wb = pv.Workbook
                except:
                    wb = pv.Edit()
            finally:
                _cleanup_com(pv)

        def _ws_used_value2(ws):
            used = None
            try:
                used = ws.UsedRange
                return used.Value2
            finally:
                _cleanup_com(used)

        if sheet_name:
            ws = None
            try:
                ws = wb.Worksheets.Item(sheet_name)
                rows = _to_rows_2d(_ws_used_value2(ws))
            finally:
                _cleanup_com(ws)
        else:
            best_v = None
            best_score = -1
            sheets = wb.Worksheets
            try:
                count = sheets.Count
                for i in range(1, count + 1):
                    ws = None
                    try:
                        ws = sheets.Item(i)
                        v = _ws_used_value2(ws)
                        score = _nonempty_cell_count(v)
                        if score > best_score:
                            best_score, best_v = score, v
                    finally:
                        _cleanup_com(ws)
            finally:
                _cleanup_com(sheets)
            rows = _to_rows_2d(best_v)

        return rows
    finally:
        try:
            if wb: wb.Close(False)
        except:
            pass
        try:
            if excel: excel.Quit()
        except:
            pass
        _cleanup_com(wb, excel)

def read_table_safely(path, sheet_name=None, retry_once=True):
    """CSV читаем без Excel; XLS/XLSX — через COM. Один ретрай на случай подвисаний."""
    path = u"%s" % path
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return read_csv_fast(path)
    try:
        rows = read_excel_via_com(path, sheet_name)
        if (rows is None or len(rows) == 0) and retry_once:
            return read_table_safely(path, sheet_name, retry_once=False)
        return rows
    except System.Runtime.InteropServices.COMException:
        if retry_once:
            return read_table_safely(path, sheet_name, retry_once=False)
        raise

# ------------ Маппинг из файла (A,B,C,D или A,C,D,E) ------------
def normalize_code(s):
    if not s: return u""
    return u"".join(ch.lower() for ch in (u"%s" % s) if ch.isalnum())

def read_mapping_from_file(path):
    rows = read_table_safely(path)

    # эвристика раскладки: если B очень короткий (<=4) и D длинный (>=10) — берём A,C,D,E
    def _is_short(s): return len((u"%s" % s).strip()) <= 4
    def _is_long(s):  return len((u"%s" % s).strip()) >= 10
    use_alt_layout = any(_is_short(B) and _is_long(D) for A,B,C,D,E in rows[:10]) if rows else False

    name_by_code = {}; code_by_code = {}; mfr_by_code = {}
    for A,B,C,D,E in rows:
        key = normalize_code(A)
        if key == u"":
            continue
        if use_alt_layout:
            # A, C, D, E -> код, ADSK_Код, ADSK_Наименование, ADSK_Завод-изготовитель
            if (u"%s" % D).strip(): name_by_code[key] = u"%s" % D
            if (u"%s" % C).strip(): code_by_code[key] = u"%s" % C
            if (u"%s" % E).strip(): mfr_by_code[key]  = u"%s" % E
        else:
            # A, B, C, D -> код, ADSK_Наименование, ADSK_Код, ADSK_Завод-изготовитель
            if (u"%s" % B).strip(): name_by_code[key] = u"%s" % B
            if (u"%s" % C).strip(): code_by_code[key] = u"%s" % C
            if (u"%s" % D).strip(): mfr_by_code[key]  = u"%s" % D
    return name_by_code, code_by_code, mfr_by_code

def find_best(code_text, mapping):
    src = normalize_code(code_text)
    if src in mapping:
        return mapping[src]
    for k, v in mapping.items():
        if k in src or src in k:
            return v
    return None

def pick_file():
    dlg = OpenFileDialog()
    dlg.Title = u"Выберите Excel/CSV с исходными данными"
    dlg.Filter = u"Excel (*.xlsx;*.xls)|*.xlsx;*.xls|CSV (*.csv)|*.csv|Все файлы (*.*)|*.*"
    dlg.Multiselect = False
    return dlg.FileName if dlg.ShowDialog() == DialogResult.OK else None

# ------------ Печать отчёта ------------
def _print_unmatched_tables(unmatched, rep_counts):
    if not _HAS_OUTPUT:
        return
    # Заголовок и счётчики
    out.print_md(u"### Итоги обработки Excel/CSV → ADSK")
    out.print_md(u"- Обработано элементов: **{0}**".format(rep_counts.get('total', 0)))
    out.print_md(u"- Записано *ADSK_Наименование*: **{0}**".format(rep_counts.get('name', 0)))
    out.print_md(u"- Записано *ADSK_Код изделия*: **{0}**".format(rep_counts.get('code', 0)))
    out.print_md(u"- Записано *ADSK_Завод-изготовитель*: **{0}**".format(rep_counts.get('mfr', 0)))
    out.print_md(u"- Скопировано *ADSK_Единица изменения*: **{0}**".format(rep_counts.get('unit', 0)))

    out.print_md(u"## Элементы, которых **нет** в исходной таблице по значению *DKC_Код изделия* (уникальные коды): **{0}**".format(len(unmatched)))
    if not unmatched:
        out.print_md(u"— Совпадения найдены для всех элементов.")
        return

    # Группируем по категории
    by_cat = {}
    for it in unmatched:
        by_cat.setdefault(it.get('cat') or u'—', []).append(it)

    for cat in sorted(by_cat.keys(), key=lambda s: s.lower() if isinstance(s, basestring) else u""):
        rows = by_cat[cat]
        out.print_md(u"#### Категория: **{0}** — {1} шт.".format(cat or u'—', len(rows)))
        table_data = []
        for i, it in enumerate(rows, 1):
            try:
                link = out.linkify(ElementId(int(it['id'])))
            except:
                link = (u"%s" % it.get('id'))
            ft = it.get('ft') or u''
            table_data.append([
                i,
                link,
                it.get('dkc') or u'—',
                ft or u'—'
            ])
        out.print_table(
            table_data=table_data,
            columns=[u'№', u'ID', u'DKC_Код из модели', u'Семейство : Тип'],
            title=None
        )
    out.print_md(u"_Клик по **ID** выделяет элемент в модели._")

# ------------ Основной процесс ------------
def process_with_mapping(name_map, code_map, mfr_map):
    elems = collect_elements(CATS)
    if not elems:
        MessageBox.Show(u"В модели не найдено элементов нужных категорий.", u"Excel/CSV → ADSK")
        return

    # Список для отчёта (дедуп по нормализованному коду)
    unmatched = []
    unmatched_seen = set()

    t = Transaction(doc, u"Excel/CSV → ADSK")
    t.Start()
    rep_total = len(elems)
    rep_name = rep_code = rep_mfr = rep_unit = 0

    for el in elems:
        typ = get_type_element(el)

        # ключ: DKC_Код изделия (несколько вариантов имени)
        src_code_text = None
        for nm in SRC_CODE_CANDIDATES:
            v, _ = get_val_and_type(lookup_param(el, nm))
            if v:
                src_code_text = v
                break
            v, _ = get_val_and_type(lookup_param(typ, nm))
            if v:
                src_code_text = v
                break

        if not src_code_text or normalize_code(src_code_text) == u"":
            # у элемента нет значения кода — игнорируем для unmatched
            continue

        # значения из файла
        name_val = find_best(src_code_text, name_map)
        code_val = find_best(src_code_text, code_map)
        mfr_val  = find_best(src_code_text, mfr_map)

        # если ни один из трёх не найден — добавим в unmatched (с дедупликацией)
        if name_val is None and code_val is None and mfr_val is None:
            normkey = normalize_code(src_code_text)
            if normkey and (normkey not in unmatched_seen):
                unmatched_seen.add(normkey)
                cat = u""
                try:
                    cat = el.Category.Name if el.Category else u""
                except:
                    pass
                ft = family_type_string(el, typ)
                unmatched.append({
                    'id': el.Id.IntegerValue,
                    'cat': cat or u'',
                    'ft': ft or u'',
                    'dkc': u"%s" % src_code_text
                })

        # Запись параметров
        if name_val is not None:
            if set_on_inst_or_type(el, DST_NAME, name_val, StorageType.String):
                rep_name += 1
        if code_val is not None:
            if set_on_inst_or_type(el, DST_CODE, code_val, StorageType.String):
                rep_code += 1
        if mfr_val is not None:
            if set_on_inst_or_type(el, DST_MFR,  mfr_val,  StorageType.String):
                rep_mfr  += 1

        # Копируем единицу изменения, если есть
        unit_src_val, unit_src_st = get_val_and_type(lookup_param(el, SRC_UNIT) or lookup_param(typ, SRC_UNIT))
        if not is_empty_value(unit_src_val, unit_src_st):
            if set_on_inst_or_type(el, DST_UNIT, unit_src_val, unit_src_st):
                rep_unit += 1

        # Количество: ДлинаФакт (→ метры) ИЛИ Количество
        wrote_qty = False
        p_len = lookup_param(el, SRC_LENFACT) or lookup_param(typ, SRC_LENFACT)
        if p_len:
            val_len, st_len = get_val_and_type(p_len)
            if not is_empty_value(val_len, st_len):
                val_m = None
                if st_len == StorageType.Double:
                    try:
                        val_m = feet_to_meters(float(val_len))
                    except:
                        val_m = None
                elif st_len == StorageType.String:
                    val_m = parse_length_string_to_meters(val_len)
                if val_m is not None:
                    try:
                        val_m = round(float(val_m), 4)
                    except:
                        pass
                    if set_on_inst_or_type(el, DST_QTY, val_m, StorageType.Double):
                        wrote_qty = True

        if not wrote_qty:
            p_qty = lookup_param(el, SRC_QTY) or lookup_param(typ, SRC_QTY)
            val_qty, st_qty = get_val_and_type(p_qty)
            if not is_empty_value(val_qty, st_qty):
                set_on_inst_or_type(el, DST_QTY, val_qty, st_qty)

    t.Commit()

    # Печать отчёта в окно pyRevit
    _print_unmatched_tables(unmatched, {
        'total': rep_total, 'name': rep_name, 'code': rep_code, 'mfr': rep_mfr, 'unit': rep_unit
    })

    # И компактное окно-резюме
    try:
        MessageBox.Show(
            u"Обработано элементов: %s\n"
            u"— Записано ADSK_Наименование: %s\n"
            u"— Записано ADSK_Код изделия: %s\n"
            u"— Записано ADSK_Завод-изготовитель: %s\n"
            u"— Скопировано ADSK_Единица изменения: %s\n"
            u"Уникальных кодов без совпадения: %s" %
            (rep_total, rep_name, rep_code, rep_mfr, rep_unit, len(unmatched)),
            u"Excel/CSV → ADSK"
        )
    except:
        pass

# ------------ Entry ------------
if __name__ == "__main__":
    path = pick_file()
    if not path:
        try:
            MessageBox.Show(u"Файл не выбран.", u"Excel/CSV → ADSK")
        except:
            pass
    else:
        try:
            name_map, code_map, mfr_map = read_mapping_from_file(path)
            if not (name_map or code_map or mfr_map):
                MessageBox.Show(
                    u"В файле не найдено пригодных строк.\n"
                    u"Проверьте, что на одном из листов есть данные в колонках A-D (код, наименование, код, завод) "
                    u"или в раскладке A,C,D,E.",
                    u"Excel/CSV → ADSK"
                )
            else:
                process_with_mapping(name_map, code_map, mfr_map)
        except System.Runtime.InteropServices.COMException as ex:
            MessageBox.Show(
                u"Не удалось прочитать Excel через COM.\n"
                u"Откройте файл один раз вручную, нажмите «Разрешить редактирование», закройте — и повторите.\n\nДетали:\n%s" % ex,
                u"Excel/CSV → ADSK"
            )
        except Exception as ex:
            MessageBox.Show(u"Не удалось прочитать файл:\n%s" % ex, u"Excel/CSV → ADSK")
