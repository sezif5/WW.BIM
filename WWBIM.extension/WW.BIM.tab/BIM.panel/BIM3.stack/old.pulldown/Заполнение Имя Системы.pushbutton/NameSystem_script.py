# -*- coding: utf-8 -*-
__title__  = "Заполнение Имя Системы"
__author__ = "Vlad"
__doc__    = ("Имя системы (все коды вида Буквы+числа[.числа]) или Сокращение для системы (все, без дублей) -> целевой параметр. "
              "Натуральная сортировка; вложенные (в т.ч. shared) наследуют, если своих значений нет; "
              "коннекторы читаем только у Механического оборудования. Shift-клик — задать имя параметра.")

from pyrevit import revit, forms, script
import Autodesk.Revit.DB as DB
from Autodesk.Revit.DB import BuiltInCategory, BuiltInParameter, FilteredElementCollector, Element, FamilyInstance

doc = revit.doc
out = script.get_output()
out.close_others(all_open_outputs=True)

# ---------------- Настройки ----------------
DEFAULT_TARGETS = (u"ADSK_Система_Имя", u"ИмяСистемы")
CONNECTORS_ONLY_FOR = (BuiltInCategory.OST_MechanicalEquipment,)

CATS = [
    BuiltInCategory.OST_DuctCurves, BuiltInCategory.OST_PipeCurves,
    BuiltInCategory.OST_FlexDuctCurves, BuiltInCategory.OST_FlexPipeCurves,
    BuiltInCategory.OST_DuctFitting, BuiltInCategory.OST_PipeFitting,
    BuiltInCategory.OST_DuctAccessory, BuiltInCategory.OST_PipeAccessory,
    BuiltInCategory.OST_DuctTerminal, BuiltInCategory.OST_PlumbingFixtures,
    BuiltInCategory.OST_Sprinklers, BuiltInCategory.OST_MechanicalEquipment,
    BuiltInCategory.OST_CableTray, BuiltInCategory.OST_DuctInsulations,
    BuiltInCategory.OST_PipeInsulations,
]

# --------------- Утилиты -------------------
import re

def _as_str(p):
    if not p: return None
    try:
        s = p.AsString()
        if s: return s.strip()
    except: pass
    try:
        s = p.AsValueString()
        if s: return s.strip()
    except: pass
    return None

def _split_list(s):
    if not s: return []
    parts = re.split(ur"[;,]+", s)
    parts = [t.strip() for t in parts if t and t.strip()]
    if len(parts) <= 1 and u" " in s.strip():
        parts = [t.strip() for t in s.split() if t.strip()]
    return parts

def _add_unique(acc, t):
    if t and t not in acc:
        acc.append(t)

def _is_equipment(el):
    try:
        return el.Category and el.Category.Id.IntegerValue == int(BuiltInCategory.OST_MechanicalEquipment)
    except:
        return False

def _safe_iter_connectors(el):
    try:
        cm = None
        if hasattr(el, "MEPModel") and el.MEPModel:
            cm = el.MEPModel.ConnectorManager
        elif hasattr(el, "ConnectorManager"):
            cm = el.ConnectorManager
        conns = getattr(cm, "Connectors", None)
        if not conns: return []
        res = []
        for c in conns:
            try: res.append(c)
            except: pass
        return res
    except:
        return []

# ---- натуральная сортировка (Т == T) ----
_CYR_TO_LAT = {u'А':u'A',u'В':u'B',u'Е':u'E',u'К':u'K',u'М':u'M',u'Н':u'H',u'О':u'O',u'Р':u'P',u'С':u'C',u'Т':u'T',u'Х':u'X',u'У':u'Y',u'Ё':u'E',
               u'а':u'A',u'в':u'B',u'е':u'E',u'к':u'K',u'м':u'M',u'н':u'H',u'о':u'O',u'р':u'P',u'с':u'C',u'т':u'T',u'х':u'X',u'у':u'Y',u'ё':u'E'}
def _norm_letters(s):
    return u"".join(_CYR_TO_LAT.get(ch, ch).upper() for ch in (s or u""))

_num_re = re.compile(ur'^\s*([A-Za-zА-Яа-яЁё]+)?\s*([0-9]+(?:\.[0-9]+)*)?\s*$')
def _token_sort_key(tok):
    t = (tok or u"").strip()
    m = _num_re.match(t)
    if not m: return (_norm_letters(t), (), t)
    letters = _norm_letters(m.group(1) or u"")
    nstr = m.group(2)
    nums = tuple(int(x) for x in nstr.split(u'.')) if nstr else ()
    return (letters, nums, t)

def _sort_tokens(tokens):
    try:    return sorted(tokens, key=_token_sort_key)
    except: return sorted(tokens)

# ---- извлечение кодов из строк имён ----
# Берём только шаблон: Буквы + цифры(.цифры) — игнорируем одиночные числа и прочий текст.
_CODE_RE = re.compile(ur'([A-Za-zА-Яа-яЁё]+[0-9]+(?:\.[0-9]+)*)')

def _codes_from_names_string(s):
    res = []
    for part in _split_list(s):
        m = _CODE_RE.search(part)
        if m:
            _add_unique(res, m.group(1).strip())
    return res

# ---- кэши и извлечение источников ----
_abbr_cache = {}
_name_cache = {}

def _abbr_from_element(el):
    eid = el.Id.IntegerValue
    if eid in _abbr_cache: return list(_abbr_cache[eid])
    acc = []
    for t in _split_list(_as_str(el.get_Parameter(BuiltInParameter.RBS_SYSTEM_ABBREVIATION_PARAM))):
        _add_unique(acc, t)
    for n in (u"Сокращение для системы", u"System Abbreviation"):
        for t in _split_list(_as_str(el.LookupParameter(n))):
            _add_unique(acc, t)
    try:
        msys = getattr(el, "MEPSystem", None)
        if msys:
            typ = doc.GetElement(msys.GetTypeId())
            if typ:
                for t in _split_list(_as_str(typ.get_Parameter(BuiltInParameter.RBS_SYSTEM_ABBREVIATION_PARAM))):
                    _add_unique(acc, t)
    except: pass
    _abbr_cache[eid] = list(acc)
    return acc

def _name_codes_from_element(el):
    eid = el.Id.IntegerValue
    if eid in _name_cache: return list(_name_cache[eid])

    acc = []
    nm = _as_str(el.get_Parameter(BuiltInParameter.RBS_SYSTEM_NAME_PARAM))
    if not nm:
        for n in (u"Имя системы", u"System Name"):
            nm = _as_str(el.LookupParameter(n))
            if nm: break
    for t in _codes_from_names_string(nm or u""):
        _add_unique(acc, t)

    try:
        msys = getattr(el, "MEPSystem", None)
        if msys:
            name = (getattr(msys, "Name", u"") or u"").strip()
            for t in _codes_from_names_string(name):
                _add_unique(acc, t)
    except: pass

    _name_cache[eid] = list(acc)
    return acc

def _abbr_from_connected_owners(el):
    acc = []
    if not _is_equipment(el): return acc
    for c in _safe_iter_connectors(el):
        refs = []
        try:
            for rc in c.AllRefs:
                refs.append(rc)
        except:
            continue
        for rc in refs:
            try:
                owner = getattr(rc, "Owner", None)
                if isinstance(owner, Element) and owner.Id != el.Id:
                    for t in _abbr_from_element(owner):
                        _add_unique(acc, t)
            except: pass
    return acc

def _name_from_connected_owners(el):
    acc = []
    if not _is_equipment(el): return acc
    for c in _safe_iter_connectors(el):
        refs = []
        try:
            for rc in c.AllRefs:
                refs.append(rc)
        except:
            continue
        for rc in refs:
            try:
                owner = getattr(rc, "Owner", None)
                if isinstance(owner, Element) and owner.Id != el.Id:
                    for t in _name_codes_from_element(owner):
                        _add_unique(acc, t)
            except: pass
    return acc

def set_target_param(el, value, targets):
    for pname in targets:
        p = el.LookupParameter(pname)
        if not p: continue
        if p.IsReadOnly:
            return False, u"параметр «{}» только для чтения".format(pname)
        try:
            p.Set(u"{}".format(value)); return True, u""
        except Exception as e:
            try:  return False, unicode(e)
            except: return False, str(e)
    return False, u"нет параметра: {}".format(u", ".join(targets))

def family_label(el):
    try:
        et = doc.GetElement(el.GetTypeId())
        fam = getattr(et, "FamilyName", None) if et else None
        typ = getattr(et, "Name", None) if et else None
        if fam and typ: return u"{} : {}".format(fam, typ)
        if fam: return fam
        if typ: return typ
    except: pass
    return (el.Category.Name if el.Category else u"")

def collect_elements():
    seen, res = set(), []
    for bic in CATS:
        for el in FilteredElementCollector(doc).OfCategory(bic).WhereElementIsNotElementType():
            if el.Category and el.Category.Id.IntegerValue == int(BuiltInCategory.OST_GenericModel):
                continue
            eid = el.Id.IntegerValue
            if eid not in seen:
                seen.add(eid); res.append(el)
    return res

def super_parent_id(el):
    if not isinstance(el, FamilyInstance): return None
    try:
        parent = el.SuperComponent
        if parent: return parent.Id.IntegerValue
    except: pass
    return None

# ----------------- UI ----------------------
opts = [
    u"Имя системы (все коды) → по первому слову",
    u"Системный параметр «Сокращение для системы» (все, без дублей)",
]
try:
    picked = forms.CommandSwitchWindow.show(opts, message=u"Источник значения")
except:
    picked = forms.SelectFromList.show(opts, title=u"Источник значения")
if not picked: script.exit()

use_abbr = (picked == opts[1])

try:    SHIFT_CLICK = bool(__shiftclick__)
except: SHIFT_CLICK = False

TARGETS = list(DEFAULT_TARGETS)
if SHIFT_CLICK:
    custom = forms.ask_for_string(
        default=TARGETS[0],
        title=__title__,
        prompt=u"Введите имя параметра для записи"
    )
    if custom: TARGETS = [custom.strip()]
TARGETS = tuple(TARGETS)

# --------- Сбор значений (вне транзакции) ---------
elements = collect_elements()

base_text = {}      # собственный расчёт по элементу
parent_map = {}     # child_eid -> parent_eid
for el in elements:
    eid = el.Id.IntegerValue
    pid = super_parent_id(el)
    if pid: parent_map[eid] = pid

    if use_abbr:
        tokens = []
        for t in _abbr_from_element(el): _add_unique(tokens, t)
        for t in _abbr_from_connected_owners(el): _add_unique(tokens, t)
    else:
        tokens = []
        for t in _name_codes_from_element(el): _add_unique(tokens, t)   # <-- берём только коды
        for t in _name_from_connected_owners(el): _add_unique(tokens, t)

    base_text[eid] = u", ".join(_sort_tokens(tokens)) if tokens else None

# Наследование: если у ребёнка пусто → берём у ближайшего родителя
final_text = dict(base_text)
changed = True
while changed:
    changed = False
    for eid, pid in parent_map.items():
        if not final_text.get(eid) and final_text.get(pid):
            final_text[eid] = final_text[pid]
            changed = True

# ---------- Запись (в транзакции) ----------
fails_read, fails_write = [], []
with revit.Transaction(u"Заполнение Имя Системы"):
    for el in elements:
        eid = el.Id.IntegerValue
        text = final_text.get(eid)
        if not text:
            fails_read.append([out.linkify(el.Id), family_label(el), u"Не найдено значение"])
            continue
        ok, reason = set_target_param(el, text, TARGETS)
        if not ok:
            fails_write.append([out.linkify(el.Id), family_label(el), reason or u""])

# ---------------- Отчёт --------------------
out.set_width(1100)
if fails_read or fails_write:
    if fails_read:
        out.print_md(u"### Не нашли, что записать")
        out.print_table(fails_read, [u"ID", u"Семейство / Тип", u"Причина"])
    if fails_write:
        out.print_md(u"### Не удалось записать")
        out.print_table(fails_write, [u"ID", u"Семейство / Тип", u"Причина"])
else:
    forms.alert(u"Готово. Все элементы обработаны.", title=__title__)
