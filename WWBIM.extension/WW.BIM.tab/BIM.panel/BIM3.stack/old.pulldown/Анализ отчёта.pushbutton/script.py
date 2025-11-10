# -*- coding: utf-8 -*-
__title__  = u"Clash Analytics (Charts)"
__author__ = u"vlad / you"
__doc__    = u"Аналитика XML-отчёта Navisworks: устойчивый парсер, графики, разделы% и этажи (строгие шаблоны), фикс. цвета статусов."

import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from pyrevit import forms, script

# ---------- Output ----------
out = script.get_output()
try:
    out.close_others(all_open_outputs=True)
except Exception:
    pass
out.set_width(1280)
out.set_height(900)

# ---------- Палитра общая ----------
_PALETTE = ["#F7921E", "#FFA74B", "#3C78D8", "#8E7CC3", "#6AA84F", "#E06666",
            "#76A5AF", "#FFD966", "#93C47D", "#A64D79", "#4C1130", "#274E13"]

def _rgba(hex_color, a):
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return "rgba(%d,%d,%d,%.3f)" % (r, g, b, a)

def _color(i, a=0.6):
    return _rgba(_PALETTE[i % len(_PALETTE)], a)

# ---------- Фиксированные цвета статусов ----------
STATUS_COLORS = {
    u"Новые"      : "#E74C3C",  # красный
    u"Активные"   : "#F39C12",  # оранжевый
    u"Проверено"  : "#00B0F0",  # голубой
    u"Согласовано": "#2ECC71",  # зелёный
    u"Решено"     : "#F1C40F",  # жёлтый
    u"Другое"     : "#95A5A6",  # серый
    u"Не указано" : "#BDC3C7",  # светло-серый
}

def _status_color(name, alpha):
    hexcol = STATUS_COLORS.get(name)
    if hexcol:
        return _rgba(hexcol, alpha)
    return _color(abs(hash(name)) % len(_PALETTE), alpha)

# ---------- XML backend ----------
try:
    from lxml import etree as ET
    _USING_LXML = True
except Exception:
    import xml.etree.ElementTree as ET
    _USING_LXML = False

# ---------- Очистка XML ----------
_BAD_AMP_RE = re.compile(u"&(?!amp;|lt;|gt;|apos;|quot;|#\d+;|#x[0-9A-Fa-f]+;)", re.UNICODE)

def _read_text_guess_enc(path):
    with open(path, "rb") as f:
        data = f.read()
    for enc in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "utf-8", "cp1251"):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode("utf-8", "ignore")

def _strip_invalid_xml_chars(s):
    out_chars = []
    for ch in s:
        c = ord(ch)
        if (c in (0x9, 0xA, 0xD)) or (0x20 <= c <= 0xD7FF) or (0xE000 <= c <= 0xFFFD) or (0x10000 <= c <= 0x10FFFF):
            out_chars.append(ch)
        else:
            out_chars.append(u" ")
    return u"".join(out_chars)

def _sanitize_xml_text(s):
    s = s.replace(u"\r\n", u"\n").replace(u"\r", u"\n")
    s = s.replace(u"\ufeff", u" ")
    s = _strip_invalid_xml_chars(s)
    s = _BAD_AMP_RE.sub(u"&amp;", s)
    p = s.find(u"<")
    if p > 0:
        s = s[p:]
    return s

def _safe_xml_root(xml_path):
    cleaned = _sanitize_xml_text(_read_text_guess_enc(xml_path))
    if _USING_LXML:
        try:
            return ET.fromstring(cleaned, parser=ET.XMLParser(recover=True, huge_tree=True))
        except Exception:
            return ET.fromstring(cleaned)
    return ET.fromstring(cleaned)

# ---------- Helpers ----------
_FILE_RX = re.compile(u".+\.(nwc|nwd|nwf|rvt)$", re.IGNORECASE)

def _img_abs_from_href(base_dir, href_rel):
    if not href_rel:
        return u""
    href_rel = (href_rel or u"").replace("\\", "/").lstrip("./")
    return os.path.normpath(os.path.join(base_dir, href_rel))

def _path_first_filename(path_text):
    if not path_text:
        return u""
    tokens = [t.strip() for t in path_text.replace("\\", "/").split(u"/")]
    for t in tokens:
        if _FILE_RX.match(t):
            return os.path.splitext(t)[0]
    return u""

def _split_path_tokens(path_text):
    if not path_text:
        return []
    p = path_text.replace("\\", "/")
    return [x.strip() for x in (p.split(" / ") if " / " in p else p.split("/")) if x.strip()]

def _pathlink_string_from_element(co):
    parts = []
    for node in co.findall("./pathlink/node"):
        t = (node.text or "").strip()
        if t:
            parts.append(t)
    if not parts:
        for node in co.findall("./pathlink/path"):
            t = (node.text or "").strip()
            if t:
                parts.append(t)
    return u" / ".join(parts)

def _object_id_from_element(co):
    for st in co.findall("./smarttags/smarttag"):
        name = (st.findtext("name") or "").strip().lower()
        if name in ("объект id", "object id", "object 1 id", "object 2 id", "id объекта", "element id", "revit id"):
            val = (st.findtext("value") or "").strip()
            if val:
                return val
    for oa in co.findall("./objectattribute"):
        name = (oa.findtext("name") or "").strip().lower()
        if name in ("id объекта", "object id", "объект id", "element id", "revit id"):
            val = (oa.findtext("value") or "").strip()
            if val:
                return val
    return None

def _status_from_clashresult(cr):
    for key in ("status", "state"):
        val = (cr.get(key) or "").strip().lower()
        if val:
            return _map_status(val)
    st = cr.find("./status")
    if st is not None:
        val = (st.text or "").strip().lower()
        if val:
            return _map_status(val)
    if (cr.get("approved") or "").lower() in ("1", "true", "yes"):
        return u"Согласовано"
    if (cr.get("resolved") or "").lower() in ("1", "true", "yes"):
        return u"Решено"
    return u"Не указано"

def _map_status(v):
    v = (v or "").lower()
    if v in ("new", "not_checked", "untested"):   return u"Новые"
    if v in ("active", "open", "in_progress"):    return u"Активные"
    if v in ("reviewed", "checked"):              return u"Проверено"
    if v in ("approved", "accepted"):             return u"Согласовано"
    if v in ("resolved", "done", "closed"):       return u"Решено"
    return u"Другое"

_DATETIME_FORMATS = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                     "%d.%m.%Y %H:%M:%S", "%d.%m.%Y", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]

def _parse_dt(s):
    s = (s or "").strip()
    if not s:
        return None
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    if s.endswith("Z"):
        try:
            return datetime.strptime(s[:-1], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    return None

def _created_dt_from_clashresult(cr):
    for key in ("created", "create", "createdate", "createdatetime", "timestamp", "time", "date"):
        val = cr.get(key)
        dt = _parse_dt(val)
        if dt:
            return dt
    for key in ("created", "date", "time"):
        el = cr.find("./" + key)
        if el is not None:
            dt = _parse_dt(el.text)
            if dt:
                return dt
    return None

# ---------- Разделы ----------
_SECTION_ALIASES = {
    u"АР": u"АР", u"АРХ": u"АР", u"AR": u"АР", u"ARCH": u"АР",
    u"ОВВ": u"ОВВ", u"ОВК": u"ОВВ", u"ОВ": u"ОВВ", u"HVAC": u"ОВВ",
    u"ВК": u"ВК", u"VK": u"ВК", u"WS": u"ВК",
    u"КР": u"КР", u"КЖ": u"КР", u"КМ": u"КР", u"KR": u"КР", u"STRUCT": u"КР",
    u"ЭОМ": u"ЭОМ", u"ЭС": u"ЭОМ", u"ЭО": u"ЭОМ", u"EOM": u"ЭОМ", u"EL": u"ЭОМ",
    u"СС": u"СС", u"СКС": u"СС", u"SS": u"СС", u"SCS": u"СС",
    u"ВИС": None, u"ИС": None
}
_SECTION_PREFIX_RX = re.compile(u"^(АР|ОВВ|ОВК|ОВ|ВК|КР|ЭОМ|СС|ВИС|ИС)[\s_\-]+",
                                re.IGNORECASE | re.UNICODE)
_HVAC_KEYS=[u"воздуховод",u"воздухораспредел",u"решет",u"диффузор",u"фасонн",u"вентил",u"дымоудал"]
_VK_KEYS  =[u"труб",u"водопровод",u"канализа",u"фитинг",u"арматур",u"отвод",u"насос",u"колодец"]
_EOM_KEYS =[u"кабель",u"лоток",u"щит",u"светиль",u"розет",u"электр",u"осветит",u"трансформ"]
_SS_KEYS  =[u"слаботоч",u"скс",u"опс",u"апс",u"соуэ",u"видеонаблюд",u"диспетчер"]
_KR_KEYS  =[u"балк",u"колон",u"плита",u"свая",u"фундамент",u"каркас",u"ферм",u"ригель",u"кж",u"км"]
_AR_KEYS  =[u"стен",u"перегород",u"двер",u"окн",u"витраж",u"потол",u"пол",u"помещен",u"архит"]

def _section_by_keywords(text):
    s = (text or u"").lower()
    def hit(keys): return any(k in s for k in keys)
    if hit(_HVAC_KEYS): return u"ОВВ"
    if hit(_VK_KEYS):   return u"ВК"
    if hit(_EOM_KEYS):  return u"ЭОМ"
    if hit(_SS_KEYS):   return u"СС"
    if hit(_KR_KEYS):   return u"КР"
    if hit(_AR_KEYS):   return u"АР"
    return None

def _canonical_section(token):
    t = (token or u"").strip().upper().replace("Ё", "Е")
    return _SECTION_ALIASES.get(t, None)

def _extract_section_from_path(path):
    toks = _split_path_tokens(path)
    file_idx = -1
    for i, tk in enumerate(toks):
        if _FILE_RX.match(tk):
            file_idx = i
            break
    if 0 <= file_idx < len(toks) - 1:
        sec_token = toks[file_idx + 1]
        code = sec_token.split("_", 1)[0]
        sec = _canonical_section(code)
        if sec:
            return sec
        tail = u" / ".join(toks[file_idx + 1:])
        sec_kw = _section_by_keywords(tail)
        if sec_kw:
            return sec_kw
    base = _path_first_filename(path)
    if base:
        for part in re.split(r"[^A-Za-zА-Яа-я0-9]+", base.upper()):
            sec = _canonical_section(part)
            if sec:
                return sec
        sec_kw = _section_by_keywords(base)
        if sec_kw:
            return sec_kw
    sec_kw = _section_by_keywords(u" / ".join(toks))
    return sec_kw or u"Прочее"

# ---------- Этажи (строгие шаблоны) ----------
_MAX_REASONABLE_FLOOR = 120
_FLOOR_PATTERNS = [
    re.compile(u"^(?:[A-Za-zА-Яа-я]{1,10}[_\-\s]*)?0*(\d{1,2})[_\-\s]*(?:этаж|эт)$", re.IGNORECASE | re.UNICODE),
    re.compile(u"^этаж[_\-\s]*0*(\-?\d{1,2})(?:[_\-\s].*)?$", re.IGNORECASE | re.UNICODE),
    re.compile(u"^(?:level|lvl|floor)[_\-\s]*0*(\d{1,2})(?:[_\-\s].*)?$", re.IGNORECASE | re.UNICODE),
]

def _special_floor(low):
    if u"подвал" in low or u"подв" in low:
        return u"Подвал"
    if u"цокол" in low:
        return u"Цоколь"
    if u"кровл" in low or u"крыша" in low:
        return u"Кровля"
    if u"техподп" in low or u"подполь" in low:
        return u"Техподполье"
    if (u"тех" in low and u"этаж" in low) or u"техэтаж" in low:
        return u"Техэтаж"
    return None

def _try_parse_floor_from_token(token):
    if not token:
        return None
    s_original = _SECTION_PREFIX_RX.sub(u"", token).strip()
    s = s_original.replace(u"—", u"-").replace(u"_", u" ").strip()
    low = s.lower()

    sf = _special_floor(low)
    if sf:
        return sf

    for rx in _FLOOR_PATTERNS:
        m = rx.match(low)
        if m:
            try:
                n = int(m.group(1))
            except Exception:
                continue
            if n < 0:
                return u"Подвал"
            if 1 <= n <= _MAX_REASONABLE_FLOOR:
                return u"%d этаж" % n
            return None
    return None

def _extract_floor_from_path(path):
    toks = _split_path_tokens(path)
    if not toks:
        return u"Пусто"

    file_idx = -1
    for i, tk in enumerate(toks):
        if _FILE_RX.match(tk):
            file_idx = i
            break

    start = (file_idx + 1) if file_idx >= 0 else 0
    primary = toks[start:start + 5]
    others  = toks[start + 5:] if start + 5 < len(toks) else []

    for cand in primary:
        res = _try_parse_floor_from_token(cand)
        if res:
            return res
    for cand in others:
        res = _try_parse_floor_from_token(cand)
        if res:
            return res

    return u"Пусто"

def _floor_sort_key(name):
    if name == u"Пусто":        return (-2, "")
    if name == u"Подвал":       return (-1, "")
    if name == u"Цоколь":       return (0, "")
    if name == u"Техподполье":  return (850, "")
    if name == u"Техэтаж":      return (900, "")
    if name == u"Кровля":       return (999, "")
    m = re.search(r"(-?\d+)", name)
    return (int(m.group(1)), "") if m else (10000, name)

# ---------- Парсинг отчёта ----------
def parse_report(xml_path):
    try:
        root = _safe_xml_root(xml_path)
        tests = _parse_report_from_root(root, os.path.dirname(xml_path))
        _ensure_all_tests_from_root(root, tests)
        return tests
    except Exception as e:
        out.print_md(u":warning: Нормальный парсер не справился — fallback. Причина: `{}`".format(e))
        tests = parse_report_fallback(xml_path)
        _ensure_all_tests_from_text(_sanitize_xml_text(_read_text_guess_enc(xml_path)), tests)
        return tests

def _parse_report_from_root(root, base_dir):
    parent = {}
    for p in root.iter():
        for ch in list(p):
            parent[ch] = p

    def _test_name_for_cr(cr):
        tn = cr.get("testname") or cr.get("test") or cr.get("groupname")
        if tn:
            return tn
        p = parent.get(cr)
        while p is not None:
            tn = p.get("name") or p.get("displayname") or p.get("testname")
            if tn:
                return tn
            p = parent.get(p)
        return u"(Без названия проверки)"

    tests = defaultdict(list)

    for cr in root.findall(".//clashresult"):
        test_name  = _test_name_for_cr(cr)
        clash_name = cr.get("name") or u"Без имени"
        status     = _status_from_clashresult(cr)
        created_dt = _created_dt_from_clashresult(cr)
        img_abs    = _img_abs_from_href(base_dir, cr.get("href") or u"")

        objs = []
        for co in cr.findall("./clashobjects/clashobject"):
            rid  = _object_id_from_element(co) or u""
            path = _pathlink_string_from_element(co)
            file = _path_first_filename(path) or u"—"
            key  = u"%s#%s" % (file, rid if rid else (path[-64:] if path else u"noid"))
            objs.append({"id": rid, "file": file, "path": path, "key": key})

        tests[test_name].append({
            "name": clash_name,
            "status": status,
            "created": created_dt,
            "img": img_abs,
            "objs": objs
        })
    return tests

# ---------- Fallback (regex) ----------
_RX_CTEST_BLOCK = re.compile(r"<clashtest\b[^>]*>.*?</clashtest>", re.DOTALL | re.IGNORECASE)
_RX_TEST_BLOCK  = re.compile(r"<test\b[^>]*>.*?</test>", re.DOTALL | re.IGNORECASE)
_RX_TEST_NAME   = re.compile(r"\bname=\"([^\"]+)\"", re.IGNORECASE)
_RX_ATTR_TESTNM = re.compile(r"\btestname=\"([^\"]+)\"", re.IGNORECASE)
_RX_CR_BLOCK    = re.compile(r"<clashresult\b[^>]*>.*?</clashresult>", re.DOTALL | re.IGNORECASE)
_RX_ATTR_HREF   = re.compile(r"\bhref=\"([^\"]*)\"", re.IGNORECASE)
_RX_ATTR_CNAME  = re.compile(r"\bname=\"([^\"]+)\"", re.IGNORECASE)
_RX_ATTR_STATUS = re.compile(r"\b(status|state)=\"([^\"]+)\"", re.IGNORECASE)
_RX_TAG_STATUS  = re.compile(r"<status>(.*?)</status>", re.DOTALL | re.IGNORECASE)
_RX_ATTR_DATE   = re.compile(r"\b(created|create|createdate|createdatetime|timestamp|time|date)=\"([^\"]+)\"", re.IGNORECASE)
_RX_TAG_DATE    = re.compile(r"<(created|date|time)>(.*?)</\1>", re.DOTALL | re.IGNORECASE)
_RX_CO_BLOCK    = re.compile(r"<clashobject\b[^>]*>.*?</clashobject>", re.DOTALL | re.IGNORECASE)
_RX_SMART_PAIR  = re.compile(r"<smarttag>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</smarttag>", re.DOTALL | re.IGNORECASE)
_RX_OA_PAIR     = re.compile(r"<objectattribute>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</objectattribute>", re.DOTALL | re.IGNORECASE)
_RX_PATHLINK    = re.compile(r"<pathlink>.*?</pathlink>", re.DOTALL | re.IGNORECASE)
_RX_NODE_TEXT   = re.compile(r"<node>(.*?)</node>", re.DOTALL | re.IGNORECASE)
_RX_TAGS        = re.compile(r"<[^>]+>")

def parse_report_fallback(xml_path):
    txt = _sanitize_xml_text(_read_text_guess_enc(xml_path))
    base_dir = os.path.dirname(xml_path)
    tests = defaultdict(list)

    blocks = list(_RX_CTEST_BLOCK.finditer(txt)) or list(_RX_TEST_BLOCK.finditer(txt)) or [None]
    for bm in blocks:
        tblock = (bm.group(0) if bm else txt)
        tname = None
        tname_m = _RX_TEST_NAME.search(tblock)
        if tname_m:
            tname = tname_m.group(1)
            tests.setdefault(tname, [])

        for crm in _RX_CR_BLOCK.finditer(tblock):
            crblock = crm.group(0)
            tnm_m = _RX_ATTR_TESTNM.search(crblock)
            test_name = tnm_m.group(1) if tnm_m else (tname or u"(Без названия проверки)")

            img_rel = (_RX_ATTR_HREF.search(crblock).group(1) if _RX_ATTR_HREF.search(crblock) else u"")
            img_abs = _img_abs_from_href(base_dir, img_rel)
            cname   = (_RX_ATTR_CNAME.search(crblock).group(1) if _RX_ATTR_CNAME.search(crblock) else u"Без имени")

            st = u"Не указано"
            st_m = _RX_ATTR_STATUS.search(crblock) or _RX_TAG_STATUS.search(crblock)
            if st_m:
                st = _map_status(st_m.groups()[-1])

            created_dt = None
            dm = _RX_ATTR_DATE.search(crblock) or _RX_TAG_DATE.search(crblock)
            if dm:
                created_dt = _parse_dt(dm.groups()[-1])

            objs = []
            for com in _RX_CO_BLOCK.finditer(crblock):
                coblock = com.group(0)
                rid = None
                for nm, val in _RX_SMART_PAIR.findall(coblock):
                    if (nm or u"").strip().lower() in ("объект id", "object id", "object 1 id", "object 2 id", "id объекта", "element id", "revit id"):
                        v = (val or u"").strip()
                        if v:
                            rid = v
                            break
                if rid is None:
                    for nm, val in _RX_OA_PAIR.findall(coblock):
                        if (nm or u"").strip().lower() in ("id объекта", "object id", "объект id", "element id", "revit id"):
                            v = (val or u"").strip()
                            if v:
                                rid = v
                                break
                path = u""
                pl = _RX_PATHLINK.search(coblock)
                if pl:
                    parts = [t.strip() for t in _RX_NODE_TEXT.findall(pl.group(0))]
                    parts = [_RX_TAGS.sub(u"", t) for t in parts if t]
                    path  = u" / ".join(parts)
                file = _path_first_filename(path) or u"—"
                key  = u"%s#%s" % (file, rid if rid else (path[-64:] if path else u"noid"))
                objs.append({"id": rid or u"", "file": file, "path": path, "key": key})

            tests[test_name].append({
                "name": cname,
                "status": st,
                "created": created_dt,
                "img": img_abs,
                "objs": objs
            })

    return tests

def _ensure_all_tests_from_root(root, tests_dict):
    for node in root.findall(".//clashtest"):
        name = node.get("name") or node.get("displayname")
        if name:
            tests_dict.setdefault(name, [])
    for node in root.findall(".//test"):
        name = node.get("name") or node.get("displayname")
        if name:
            tests_dict.setdefault(name, [])

def _ensure_all_tests_from_text(txt, tests_dict):
    for m in _RX_CTEST_BLOCK.finditer(txt):
        tn = _RX_TEST_NAME.search(m.group(0))
        if tn:
            tests_dict.setdefault(tn.group(1), [])
    for m in _RX_TEST_BLOCK.finditer(txt):
        tn = _RX_TEST_NAME.search(m.group(0))
        if tn:
            tests_dict.setdefault(tn.group(1), [])

# ---------- Aggregations ----------
def agg_by_test(tests):
    labels, cnt_collisions, cnt_parts, cnt_unique = [], [], [], []
    all_statuses = set()
    status_by_test = []
    for tname, collisions in tests.items():
        labels.append(tname)
        cnt_collisions.append(len(collisions))
        parts = 0
        uniq = set()
        st_counter = Counter()
        for col in collisions:
            parts += len(col["objs"])
            for ob in col["objs"]:
                uniq.add(ob["key"])
            st_counter[col["status"]] += 1
        cnt_parts.append(parts)
        cnt_unique.append(len(uniq))
        status_by_test.append(st_counter)
        all_statuses.update(st_counter.keys())
    pref = [u"Новые", u"Активные", u"Проверено", u"Согласовано", u"Решено", u"Другое", u"Не указано"]
    ordered = [s for s in pref if s in all_statuses] + [s for s in sorted(all_statuses) if s not in pref]
    status_matrix = [(st, [stc.get(st, 0) for stc in status_by_test]) for st in ordered]
    return labels, cnt_collisions, cnt_parts, cnt_unique, status_matrix

def agg_by_file(tests):
    parts_by_file = Counter()
    uniq_by_file_sets = defaultdict(set)
    pair_by_files = Counter()
    for cols in tests.values():
        for c in cols:
            files_in_collision = set()
            for ob in c["objs"]:
                parts_by_file[ob["file"]] += 1
                uniq_by_file_sets[ob["file"]].add(ob["key"])
                if ob["file"]:
                    files_in_collision.add(ob["file"])
            files = sorted(list(files_in_collision))
            if len(files) == 2:
                pair_by_files[u"%s ⟷ %s" % (files[0], files[1])] += 1
            elif len(files) > 2:
                for i in range(len(files)):
                    for j in range(i + 1, len(files)):
                        pair_by_files[u"%s ⟷ %s" % (files[i], files[j])] += 1
    uniq_counts = {f: len(s) for f, s in uniq_by_file_sets.items()}
    return parts_by_file, uniq_counts, pair_by_files

def agg_by_day(tests):
    by_day = Counter()
    for cols in tests.values():
        for c in cols:
            if c["created"]:
                by_day[c["created"].date()] += 1
    return by_day

def agg_by_section_percent(tests):
    cnt = Counter()
    total = 0
    for cols in tests.values():
        for c in cols:
            for ob in c["objs"]:
                cnt[_extract_section_from_path(ob["path"])] += 1
                total += 1
    if total == 0:
        return [], []
    items = list(cnt.items())
    items.sort(key=lambda kv: (kv[0] == u"Прочее", -kv[1]))
    labels = [k for k, _ in items]
    percs = [round(v * 100.0 / total, 1) for _, v in items]
    return labels, percs

def agg_by_floor(tests):
    cnt = Counter()
    for cols in tests.values():
        for c in cols:
            if c["status"] == u"Решено":
                continue
            floors = set()
            for ob in c["objs"]:
                floors.add(_extract_floor_from_path(ob["path"]))
            if not floors:
                cnt[u"Пусто"] += 1
            else:
                for f in floors:
                    cnt[f] += 1
    return cnt

# ---------- Charts ----------
def _bar_multi_chart(title, labels, series):
    chart = out.make_bar_chart()
    chart.set_style("height:320px")
    chart.options.title = {"display": True, "text": title}
    chart.options.legend = {"position": "bottom"}
    chart.options.scales = {"xAxes": [{"ticks": {"autoSkip": False}}],
                            "yAxes": [{"ticks": {"beginAtZero": True}}]}
    chart.data.labels = labels
    for i, (name, vals) in enumerate(series):
        ds = chart.data.new_dataset(name)
        ds.data = vals
        ds.backgroundColor = [_color(i, 0.45)] * len(vals)
        ds.borderColor     =  _color(i, 0.95)
        ds.borderWidth = 1
    chart.draw()

def _bar_stacked_chart(title, labels, status_matrix):
    chart = out.make_bar_chart()
    chart.set_style("height:320px")
    chart.options.title = {"display": True, "text": title}
    chart.options.legend = {"position": "bottom"}
    chart.options.tooltips = {"mode": "index", "intersect": False}
    chart.options.scales = {"xAxes": [{"stacked": True, "ticks": {"autoSkip": False}}],
                            "yAxes": [{"stacked": True, "ticks": {"beginAtZero": True}}]}
    chart.data.labels = labels
    for i, (st_name, vals) in enumerate(status_matrix):
        base_hex = STATUS_COLORS.get(st_name, _PALETTE[i % len(_PALETTE)])
        ds = chart.data.new_dataset(st_name)
        ds.data = vals
        ds.backgroundColor = _rgba(base_hex, 0.50)
        ds.borderColor     = _rgba(base_hex, 0.90)
        ds.borderWidth = 1
    chart.draw()

def _pie_or_doughnut(title, pairs, doughnut=False):
    labels = [k for k, _ in pairs]
    values = [v for _, v in pairs]
    chart = out.make_doughnut_chart() if doughnut else out.make_pie_chart()
    chart.set_style("height:280px")
    chart.options.title = {"display": True, "text": title}
    chart.options.legend = {"position": "bottom"}
    chart.data.labels = labels
    ds = chart.data.new_dataset("Количество")
    ds.data = values
    ds.backgroundColor = [_color(i, 0.7) for i in range(len(values))]
    ds.borderColor     = [_color(i, 1.0) for i in range(len(values))]
    ds.borderWidth = 1
    chart.draw()

def _line_chart(title, labels, values):
    chart = out.make_line_chart()
    chart.set_style("height:280px")
    chart.options.title = {"display": True, "text": title}
    chart.options.legend = {"display": False}
    chart.options.scales = {"yAxes": [{"ticks": {"beginAtZero": True}}]}
    chart.data.labels = labels
    ds = chart.data.new_dataset("Коллизий в день")
    ds.data = values
    ds.fill = False
    ds.borderColor = _color(0, 0.95)
    ds.backgroundColor = _color(0, 0.25)
    ds.pointRadius = 3
    chart.draw()

def _bar_percent_chart(title, labels, percents):
    chart = out.make_bar_chart()
    chart.set_style("height:300px")
    chart.options.title = {"display": True, "text": title}
    chart.options.legend = {"display": False}
    chart.options.scales = {"xAxes": [{"ticks": {"autoSkip": False}}],
                            "yAxes": [{"ticks": {"beginAtZero": True, "max": 100}}]}
    chart.data.labels = labels
    ds = chart.data.new_dataset("Доля, %")
    ds.data = percents
    ds.backgroundColor = [_color(2, 0.45)] * len(percents)
    ds.borderColor     =  _color(2, 0.95)
    ds.borderWidth = 1
    chart.draw()

# ---------- MAIN ----------
def main():
    xml_path = forms.pick_file(files_filter="XML (*.xml)|*.xml",
                               title=u"Выберите XML отчёт Navisworks")
    if not xml_path:
        forms.alert(u"Файл не выбран.", title=u"Clash Analytics")
        return

    out.print_md(u"# Аналитика отчёта по коллизиям")
    out.print_md(u"_Дата формирования отчёта:_ **%s**" % datetime.now().strftime("%Y-%m-%d %H:%M"))
    out.print_md(u"_Анализируется весь отчёт без учёта открытого файла Revit._")

    tests = parse_report(xml_path)
    if not tests:
        forms.alert(u"В отчёте не найдено ни одной проверки.", title=u"Clash Analytics")
        return

    all_tests = sorted(tests.keys(), key=lambda s: s.lower())
    picked = forms.SelectFromList.show(
        all_tests, multiselect=True,
        title=u"Выберите проверки (по умолчанию — все). Вверху есть строка поиска.",
        button_name=u"Построить графики"
    )
    if picked and len(picked) < len(all_tests):
        tests = {t: tests.get(t, []) for t in picked}

    labels, cnt_collisions, cnt_parts, cnt_unique, status_matrix = agg_by_test(tests)
    parts_by_file, uniq_by_file, pair_by_files = agg_by_file(tests)
    timeline = agg_by_day(tests)

    total_collisions = sum(cnt_collisions)
    total_parts = sum(cnt_parts)
    grand_unique = set()
    for tcols in tests.values():
        for c in tcols:
            for ob in c["objs"]:
                grand_unique.add(ob["key"])
    total_unique = len(grand_unique)

    out.print_md(u"**Итоги по выбранным проверкам:**")
    out.print_md(u"- Коллизий: **%d**" % total_collisions)
    out.print_md(u"- Участий элементов: **%d**" % total_parts)
    out.print_md(u"- Уникальных элементов: **%d**" % total_unique)
    out.print_md(u"- Проверок: **%d**" % len(tests))
    out.print_md(u"---\n## Графики")

    if labels:
        order = sorted(range(len(labels)), key=lambda i: cnt_collisions[i], reverse=True)
        lbl_sorted  = [labels[i] for i in order]
        col_sorted  = [cnt_collisions[i] for i in order]
        part_sorted = [cnt_parts[i] for i in order]
        uniq_sorted = [cnt_unique[i] for i in order]
        _bar_multi_chart(u"Проверки: коллизии vs участия vs уникальные элементы",
                         lbl_sorted,
                         [(u"Коллизии", col_sorted),
                          (u"Участий элементов", part_sorted),
                          (u"Уникальные элементы", uniq_sorted)])

    if any(sum(vals) > 0 for _, vals in status_matrix):
        order_map = {name: idx for idx, name in enumerate(labels)}
        status_sorted = []
        for st_name, vals in status_matrix:
            reordered = [0] * len(vals)
            for i, v in enumerate(vals):
                new_i = order_map.get(labels[i], i)
                reordered[new_i] = v
            status_sorted.append((st_name, reordered))
        _bar_stacked_chart(u"Статусы коллизий по проверкам (stacked)", lbl_sorted, status_sorted)

    if parts_by_file:
        top_parts = sorted(parts_by_file.items(), key=lambda x: x[1], reverse=True)[:12]
        _pie_or_doughnut(u"Участия элементов по файлам (топ)", top_parts, doughnut=True)

    if uniq_by_file:
        top_uniq = sorted(uniq_by_file.items(), key=lambda x: x[1], reverse=True)[:12]
        _pie_or_doughnut(u"Уникальные элементы по файлам (топ)", top_uniq, doughnut=False)

    if pair_by_files:
        top_pairs = sorted(pair_by_files.items(), key=lambda x: x[1], reverse=True)[:12]
        _bar_multi_chart(u"Пары файлов: число коллизий (топ)",
                         [k for k, _ in top_pairs],
                         [(u"Коллизии", [v for _, v in top_pairs])])

    if len(timeline) >= 3:
        days = sorted(timeline.keys())
        _line_chart(u"Динамика создания коллизий по дням",
                    [d.strftime("%Y-%m-%d") for d in days],
                    [timeline[d] for d in days])

    sect_labels, sect_perc = agg_by_section_percent(tests)
    if sect_labels:
        _bar_percent_chart(u"Разделы, % (по участиям элементов)", sect_labels, sect_perc)

    floor_cnt = agg_by_floor(tests)
    if floor_cnt:
        items = sorted(floor_cnt.items(), key=lambda kv: _floor_sort_key(kv[0]))
        _bar_multi_chart(u"Коллизии по этажам",
                         [k for k, _ in items],
                         [(u"Коллизии", [v for _, v in items])])

    out.print_md(u"Легенда кликабельна, значения показываются во всплывающих подсказках.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        forms.alert(u"Во время выполнения произошла ошибка.\n{0}\n(Подробности — в окне вывода)".format(e),
                    title=u"Clash Analytics")
        out.print_md("```\n" + traceback.format_exc() + "\n```")
