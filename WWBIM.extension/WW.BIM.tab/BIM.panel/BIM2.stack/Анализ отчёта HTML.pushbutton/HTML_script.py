# -*- coding: utf-8 -*-
# pyRevit pushbutton: HTML-отчёт по коллизиям (Navisworks XML) + Динамика (last 5) + Превью снимков конфликтов
from __future__ import unicode_literals

import os
import re
import io
import json
import datetime
import xml.etree.ElementTree as ET

# pyRevit
try:
    from pyrevit import forms, script
except Exception:
    forms = None
    script = None

# -----------------------------
# Палитра статусов
# -----------------------------
STATUS_COLORS = {
    u'Создать':        u'#FF3B30',
    u'Активные':       u'#FF9500',
    u'Проверенные':    u'#1DA1F2',
    u'Подтвержденные': u'#34C759',
    u'Исправленные':   u'#FFD60A',
}

def map_status(status_attr, status_text):
    s = (status_attr or u'').strip().lower()
    if s == u'active':   return u'Активные'
    if s == u'reviewed': return u'Проверенные'
    if s == u'approved': return u'Подтвержденные'
    if s == u'resolved': return u'Исправленные'
    if s == u'new':      return u'Создать'
    t = (status_text or u'').strip().lower()
    if t.startswith(u'актив'):        return u'Активные'
    if t.startswith(u'проанализ') or t.startswith(u'проверен'): return u'Проверенные'
    if t.startswith(u'подтверж'):     return u'Подтвержденные'
    if t.startswith(u'исправ'):       return u'Исправленные'
    return u'Создать'

SECTION_ALIASES = {u'АР':u'АР',u'AR':u'АР',u'ОВВ':u'ОВВ',u'VENT':u'ОВВ',u'ОВО':u'ОВО',u'OT':u'ОВО',u'ВКВ':u'ВКВ',u'VKV':u'ВКВ',u'ВКК':u'ВКК',u'VKK':u'ВКК',
                   u'КР':u'КР',u'KR':u'КР',u'СС':u'СС',u'SS1':u'СС',u'SS2':u'СС',u'ЭОМ':u'ЭОМ',u'EOM1':u'ЭОМ',u'EOM2':u'ЭОМ',u'ПТ':u'ПТ',u'PT':u'ПТ',u'ИТП':u'ИТП',
                   u'ОВ':u'ОВ', u'ВК':u'ВК'}

def section_from_filename(fname):
    name = (fname or u'').upper()
    parts = re.split(r'[_\W]+', name)
    for k in SECTION_ALIASES:
        if k in parts:
            return SECTION_ALIASES[k]
    return u'Прочее'

def normalize_floor(raw):
    s = (raw or u'').strip()
    if not s: return u'Нет уровня'
    s_low = s.lower()
    if u'кровл' in s_low: return u'Кровля'
    if u'подвал' in s_low or u'цок' in s_low: return u'Подвал'
    if s_low.startswith(u'отм'): return u'0 этаж'
    m = re.search(ur'[_\s\-]0*([0-9]{1,3})[_\s\-]*этаж', s_low)
    if m:
        n=int(m.group(1)); return u'%d этаж'%n if n<=150 else u'Нет уровня'
    m = re.search(ur'этаж[_\s\-]*0*([0-9]{1,3})', s_low)
    if m:
        n=int(m.group(1)); return u'%d этаж'%n if n<=150 else u'Нет уровня'
    return s

# -----------------------------
# Чтение и «чистка» XML (IronPython-safe)
# -----------------------------
_ILLEGAL_XML_10 = re.compile(u'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')
_UNESCAPED_AMP = re.compile(u'&(?!#\\d+;|#x[0-9A-Fa-f]+;|amp;|lt;|gt;|apos;|quot;)')

def _guess_encoding(raw_bytes):
    try:
        if raw_bytes[:2] in ('\xff\xfe', '\xfe\xff'):
            return 'utf-16'
        if raw_bytes.startswith('\xef\xbb\xbf'):
            return 'utf-8-sig'
        m = re.search(r'^<\?xml[^>]*encoding=[\'"]([A-Za-z0-9_\-]+)[\'"]', raw_bytes[:200], re.I)
        if m:
            return m.group(1)
    except Exception:
        pass
    try:
        raw_bytes.decode('utf-8')
        return 'utf-8'
    except Exception:
        return 'cp1251'

def _load_and_sanitize_xml_text(xml_path):
    with io.open(xml_path, 'rb') as f:
        raw = f.read()
    enc = _guess_encoding(raw)
    try:
        txt = raw.decode(enc, errors='replace')
    except Exception:
        try:
            txt = unicode(raw)
        except Exception:
            txt = raw
    txt = _ILLEGAL_XML_10.sub(u'', txt)
    txt = _UNESCAPED_AMP.sub(u'&amp;', txt)
    return txt

# -----------------------------
# ElementTree helpers
# -----------------------------
def _lname(tag):
    if not tag: return u''
    if isinstance(tag, tuple): tag = tag[1]
    return tag.split('}',1)[-1] if '}' in tag else tag

def _first_child_by_localname(elem, local):
    for c in list(elem):
        if _lname(getattr(c,'tag',u'')) == local:
            return c
    return None

def _iter_nodes_texts_et(pl_elem):
    out = []
    if pl_elem is None:
        return out
    for n in pl_elem.iter():
        if _lname(getattr(n,'tag',u'')) == 'node':
            t = (n.text or u'').strip()
            if t:
                out.append(t)
    return out

def _token_is_filename(t):
    m = re.search(ur'([^\s\\/<>"]+\.(?:nwc|nwd|nwf|rvt))', t, re.I|re.U)
    return m.group(1) if m else None

def _extract_from_path_nodes(nodes):
    """Возвращает (filename, floor, category) из списка node-токенов pathlink.
       БОЛЕЕ ТЕРПИМЫЙ: если файл не нашли по расширению, пробуем взять
       третий узел после 'Файл'/'Файл' и использовать его текст как имя файла."""
    fname = u''; floor = u''; cat = u''; fi = -1
    for i, t in enumerate(nodes):
        v = _token_is_filename(t)
        if v:
            fname = v; fi = i; break
    if fi < 0:
        if len(nodes) >= 3 and nodes[0].lower()==u'файл' and nodes[1].lower()==u'файл':
            guess = nodes[2].strip()
            if guess:
                fname = guess
                fi = 2
    floor_idx = None
    if fi >= 0:
        for j in range(fi+1, len(nodes)):
            tj = nodes[j]
            if re.search(ur'(этаж|отм\.?|уров)', tj, re.I|re.U):
                floor = normalize_floor(tj); floor_idx = j; break
        if floor_idx is None and len(nodes) > fi+1:
            floor = normalize_floor(nodes[fi+1]); floor_idx = fi+1
    if floor_idx is not None and len(nodes) > floor_idx+1:
        cat = nodes[floor_idx+1]
    return fname, floor, cat

def _find_object_id_et(parent):
    for el in parent.iter():
        if _lname(getattr(el,'tag',u'')) == 'smarttag':
            nm = _first_child_by_localname(el, 'name')
            if nm is not None and (nm.text or '').strip().lower() in (u'объект id', u'object id'):
                val = _first_child_by_localname(el, 'value')
                if val is not None and val.text and val.text.strip():
                    return val.text.strip()
        if _lname(getattr(el,'tag',u'')) in ('property','objectattribute'):
            nm = _first_child_by_localname(el, 'name')
            if nm is not None and (nm.text or '').strip().lower() in (u'объект id', u'object id', u'id объекта', u'ид объекта', u'id обьекта'):
                val = _first_child_by_localname(el, 'value')
                if val is not None and val.text and val.text.strip():
                    return val.text.strip()
    return None

# -----------------------------
# .NET (XDocument) helpers
# -----------------------------
def dn_first_child_local(el, local):
    try:
        for ch in el.Elements():
            if ch.Name.LocalName == local:
                return ch
    except Exception:
        pass
    return None

def dn_text_of(el):
    if el is None: return u''
    buf = []
    try:
        for nd in el.DescendantNodesAndSelf():
            nname = nd.GetType().Name
            if nname in ('XText','XCData'):
                buf.append(nd.Value)
    except Exception:
        try:
            v = getattr(el, 'Value', None)
            if v:
                buf.append(v)
        except Exception:
            pass
    return u''.join(buf)

def _iter_nodes_texts_dn(pl_elem):
    out = []
    try:
        for n in pl_elem.Descendants():
            try:
                if n.Name.LocalName != 'node':
                    continue
                t = (dn_text_of(n) or u'').strip()
                if t:
                    out.append(t)
            except Exception:
                continue
    except Exception:
        pass
    return out

def _extract_from_pathlink_dn(pl_elem):
    nodes = _iter_nodes_texts_dn(pl_elem)
    return _extract_from_path_nodes(nodes)

def dn_find_object_id(parent):
    try:
        for st in parent.Descendants():
            try:
                if st.Name.LocalName != 'smarttag':
                    continue
                nm = dn_first_child_local(st, 'name')
                if nm is None:
                    continue
                ntext = (dn_text_of(nm) or u'').strip().lower()
                if ntext in (u'объект id', u'object id'):
                    val = dn_first_child_local(st, 'value')
                    if val is not None:
                        v = (dn_text_of(val) or u'').strip()
                        if v:
                            return v
            except Exception:
                continue
    except Exception:
        pass
    try:
        for el in parent.Descendants():
            try:
                if el.Name.LocalName not in ('property','objectattribute'):
                    continue
                nm = dn_first_child_local(el, 'name')
                if nm is None:
                    continue
                ntext = (dn_text_of(nm) or u'').strip().lower()
                if ntext in (u'объект id', u'object id', u'id объекта', u'ид объекта', u'id обьекта'):
                    val = dn_first_child_local(el, 'value')
                    if val is not None:
                        v = (dn_text_of(val) or u'').strip()
                        if v:
                            return v
            except Exception:
                continue
    except Exception:
        pass
    return None

# -----------------------------
# Парсинг XML
# -----------------------------
def _parse_with_elementtree(xml_text):
    root = ET.fromstring(xml_text)
    results = []
    for ct in root.iter():
        if _lname(getattr(ct,'tag',u'')) != 'clashtest':
            continue
        testname = (ct.get('name') or u'')
        for cr in ct.iter():
            if _lname(getattr(cr,'tag',u'')) != 'clashresult':
                continue

            st_attr = (cr.get('status') or u'').strip()
            rs = _first_child_by_localname(cr, 'resultstatus')
            st_text = rs.text if (rs is not None and rs.text) else u''
            status = map_status(st_attr, st_text)
            cname = (cr.get('name') or u'')
            href = (cr.get('href') or u'').replace('\\', '/')

            objs = _first_child_by_localname(cr, 'clashobjects')
            if objs is None:
                continue
            cobjs = [c for c in list(objs) if _lname(getattr(c,'tag',u''))=='clashobject']
            if len(cobjs) < 2:
                continue

            pl1 = _first_child_by_localname(cobjs[0], 'pathlink')
            pl2 = _first_child_by_localname(cobjs[1], 'pathlink')
            n1 = _iter_nodes_texts_et(pl1)
            n2 = _iter_nodes_texts_et(pl2)
            t1 = u'\n'.join(n1); t2 = u'\n'.join(n2)
            sig = u'||'.join(sorted([t1, t2]))
            ida = _find_object_id_et(cobjs[0]) or u''
            idb = _find_object_id_et(cobjs[1]) or u''
            idsig = u'||'.join(sorted([ida, idb])) if (ida and idb) else u''

            f1, fl1, cat1 = _extract_from_path_nodes(n1)
            f2, fl2, cat2 = _extract_from_path_nodes(n2)
            sec1 = section_from_filename(f1)
            sec2 = section_from_filename(f2)
            paircats = u' — '.join(sorted([cat1 or u'', cat2 or u''])).strip(' — ')

            results.append({'status':status, 'fileA':f1,'fileB':f2,
                            'sectionA':sec1,'sectionB':sec2,'floorA':fl1,'floorB':fl2,
                            'paircats':paircats, 'sig':sig, 't1':t1, 't2':t2, 'cname':cname, 'ida':ida, 'idb':idb, 'idsig':idsig,
                            'catA':cat1, 'catB':cat2, 'testname': testname, 'href': href})
    return results

def _parse_with_dotnet(xml_text):
    import clr
    clr.AddReference('System')
    clr.AddReference('System.Xml')
    clr.AddReference('System.Xml.Linq')
    from System.IO import StringReader
    from System.Xml import XmlReader, XmlReaderSettings, DtdProcessing
    from System.Xml.Linq import XDocument

    settings = XmlReaderSettings()
    settings.CheckCharacters = False
    settings.DtdProcessing = DtdProcessing.Ignore

    reader = XmlReader.Create(StringReader(xml_text), settings)
    xdoc = XDocument.Load(reader)

    def descendants_by_local(el, local):
        for d in el.Descendants():
            try:
                if d.Name.LocalName == local:
                    yield d
            except Exception:
                pass

    results=[]
    root = xdoc.Root
    for ct in descendants_by_local(root, 'clashtest'):
        try:
            testname = (ct.Attribute('name').Value if ct.Attribute('name') is not None else u'')
        except Exception:
            testname = u''
        for cr in descendants_by_local(ct, 'clashresult'):
            try:
                st_attr = (cr.Attribute('status').Value if cr.Attribute('status') is not None else u'')
            except Exception:
                st_attr = u''
            rs = dn_first_child_local(cr,'resultstatus')
            st_text = dn_text_of(rs) if rs is not None else u''
            status = map_status(st_attr, st_text)
            try:
                cname = (cr.Attribute('name').Value if cr.Attribute('name') is not None else u'')
            except Exception:
                cname = u''
            try:
                href = (cr.Attribute('href').Value if cr.Attribute('href') is not None else u'')
            except Exception:
                href = u''
            href = href.replace('\\', '/')

            objs = dn_first_child_local(cr, 'clashobjects')
            if objs is None:
                continue
            cobjs = [c for c in objs.Elements() if c.Name.LocalName=='clashobject']
            if len(cobjs) < 2:
                continue

            pl1 = dn_first_child_local(cobjs[0], 'pathlink')
            pl2 = dn_first_child_local(cobjs[1], 'pathlink')
            n1 = _iter_nodes_texts_dn(pl1)
            n2 = _iter_nodes_texts_dn(pl2)
            t1 = u'\n'.join(n1); t2 = u'\n'.join(n2)
            sig = u'||'.join(sorted([t1, t2]))
            ida = dn_find_object_id(cobjs[0]) or u''
            idb = dn_find_object_id(cobjs[1]) or u''
            idsig = u'||'.join(sorted([ida, idb])) if (ida and idb) else u''

            f1, fl1, cat1 = _extract_from_path_nodes(n1)
            f2, fl2, cat2 = _extract_from_path_nodes(n2)
            sec1 = section_from_filename(f1)
            sec2 = section_from_filename(f2)
            paircats = u' — '.join(sorted([cat1 or u'', cat2 or u''])).strip(' — ')

            results.append({'status':status,'fileA':f1,'fileB':f2,
                            'sectionA':sec1,'sectionB':sec2,'floorA':fl1,'floorB':fl2,
                            'paircats':paircats, 'sig':sig, 't1':t1, 't2':t2, 'cname':cname, 'ida':ida, 'idb':idb, 'idsig':idsig,
                            'catA':cat1, 'catB':cat2, 'testname': testname, 'href': href})
    return results

def parse_xml(xml_path):
    xml_text = _load_and_sanitize_xml_text(xml_path)
    try:
        rows = _parse_with_elementtree(xml_text)
        if rows:
            return rows
    except Exception:
        pass
    try:
        rows = _parse_with_dotnet(xml_text)
        return rows
    except Exception as ex2:
        raise Exception(u'Ошибка разбора даже через .NET XmlReader: {0}'.format(ex2))


# -----------------------------
# Выбор периода через pyRevit.forms.SelectFromList (совместимо со старыми версиями)
# -----------------------------
def _parse_date_string(s):
    if not s:
        return None
    s = s.strip()
    for fmt in (u'%d.%m.%Y', u'%d.%m.%y', u'%Y-%m-%d', u'%Y.%m.%d', u'%d/%m/%Y', u'%d-%m-%Y'):
        try:
            d = datetime.datetime.strptime(s, fmt)
            return datetime.datetime(d.year, d.month, d.day, 0, 0, 0)
        except Exception:
            pass
    return None

def _month_start(dt):
    return datetime.datetime(dt.year, dt.month, 1, 0, 0, 0)

class _Preset(object):
    def __init__(self, text, key):
        self.name = text
        self.key = key
    def __unicode__(self):
        return self.name
    def __str__(self):
        try:
            return self.name.encode('utf-8')
        except Exception:
            return self.name

def _ask_since_select(ref_dt):
    if forms is None:
        return None
    items = [
        _Preset(u'7 дней', u'LAST_7'),
        _Preset(u'14 дней', u'LAST_14'),
        _Preset(u'30 дней', u'LAST_30'),
        _Preset(u'С начала месяца', u'MONTH_START'),
        _Preset(u'Ручной ввод…', u'MANUAL'),
        _Preset(u'Без фильтра', u'NO_FILTER'),
    ]
    try:
        sel = forms.SelectFromList.show(
            items,
            multiselect=False,
            title=u'Период для Динамики',
            button_name=u'Выбрать',
            width=350,
            height=300,
            description=u'Дата отчёта: {0}\nВыберите период:'.format(ref_dt.strftime('%d.%m.%Y'))
        )
    except Exception:
        sel = None
    key = getattr(sel, 'key', None) if sel else None

    if key == u'LAST_7':
        return ref_dt - datetime.timedelta(days=7)
    if key == u'LAST_14':
        return ref_dt - datetime.timedelta(days=14)
    if key == u'LAST_30':
        return ref_dt - datetime.timedelta(days=30)
    if key == u'MONTH_START':
        return _month_start(ref_dt)
    if key == u'MANUAL':
        try:
            s = forms.ask_for_string(default=u'', title=u'Введите дату (например, 24.09.2025)', prompt=u'Форматы: ДД.ММ.ГГГГ, YYYY-MM-DD и т.п.')
        except Exception:
            s = None
        return _parse_date_string(s) if s else None
    return None
# -----------------------------
# Поиск исторических отчётов (берём максимум 5 последних <= основному)
# -----------------------------

def find_history_reports(main_xml_path, limit=5, since_dt=None):
    main_xml_path = os.path.abspath(main_xml_path)
    main_name = os.path.basename(main_xml_path)
    current_dir = os.path.dirname(main_xml_path)            # ...\YYYY.MM.DD
    base_dir = os.path.dirname(current_dir)                 # ...\Отчёт для ВК
    if not os.path.isdir(base_dir):
        return []

    try:
        main_mtime = os.path.getmtime(main_xml_path)
    except Exception:
        main_mtime = None

    hits = []
    for root, dirs, files in os.walk(base_dir):
        for f in files:
            if f.lower() == main_name.lower():
                p = os.path.join(root, f)
                try:
                    mtime = os.path.getmtime(p)
                except Exception:
                    continue
                if (main_mtime is None) or (mtime <= main_mtime):
                    hits.append((p, mtime))

    if since_dt is not None and main_mtime is not None:
        try:
            main_dt = datetime.datetime.fromtimestamp(main_mtime)
        except Exception:
            main_dt = None
        if main_dt is not None:
            def _in_range(mt):
                try:
                    dt = datetime.datetime.fromtimestamp(mt)
                    return (dt >= since_dt and dt <= main_dt)
                except Exception:
                    return False
            hits = [(p, mt) for (p, mt) in hits if _in_range(mt)]
    else:
        hits.sort(key=lambda x: x[1])
        if len(hits) > limit:
            hits = hits[-limit:]

    hits.sort(key=lambda x: x[1])

    history = []
    for p, mt in hits:
        try:
            rows = parse_xml(p)
            ts = datetime.datetime.fromtimestamp(mt).strftime('%Y-%m-%d %H:%M')
            history.append({'path': p, 'ts': ts, 'rows': rows})
        except Exception:
            continue
    return history

# -----------------------------
# HTML
# -----------------------------
HTML_TEMPLATE = u"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Аналитика отчёта по коллизиям (HTML)</title>
<style>
  :root { --gap:16px; --card:#0b1225; --muted:#6b7280; --bg:#0f172a; }
  body { margin:0; background:#0b1120; color:#e5e7eb; font:14px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Inter,Helvetica,Arial; }
  .bar { background:#0b1225; padding:10px 16px; font-weight:600; color:#cbd5e1; display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .bar .tabs{display:flex; gap:8px;}
  .bar .tab{background:#111827;border:1px solid #1f2937;border-radius:8px;padding:8px 12px;color:#cbd5e1;cursor:pointer;}
  .bar .tab.active{background:#1f2937;color:#fff;border-color:#334155;}
  .bar .ts{font-weight:500;opacity:.8;}

  /* hero header */
  .hero{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:18px 16px;border-bottom:1px solid #1f2937;background:#0b1120;}
  .hero-left{min-width:0}
  .hero-title{font-size:20px;font-weight:800;line-height:1.15;color:#e5e7eb;word-break:break-all}
  .hero-sub{margin-top:4px;color:#93a2b7;font-size:12px}
  .hero-badge{border:1px solid #334155;background:#0f172a;color:#cbd5e1;border-radius:999px;padding:6px 10px;font-weight:600;font-size:12px;white-space:nowrap}

  .wrap { padding:16px; display:grid; grid-gap:16px; grid-template-columns:280px 1fr; }
  .panel { background:#0b1225; border:1px solid #1f2937; border-radius:10px; }
  .pad { padding:14px 16px; }
  .h { margin:0 0 12px; font-size:16px; font-weight:700; color:#e5e7eb; }
  .muted { color:#93a2b7; font-size:12px; }
  .ctrls .g { padding:10px 14px; border-bottom:1px solid #1f2937; }
  .ctrls .h2{font-weight:700;margin:0 0 8px;color:#cbd5e1;}
  .chips { display:flex; flex-wrap:wrap; gap:8px 12px; }
  .chips label { display:inline-flex; align-items:center; gap:6px; cursor:pointer; }
  .chips input { width:16px; height:16px; }
  .btns{ display:flex; gap:8px; margin-top:8px;}
  .btn { padding:6px 10px; border:1px solid #334155; border-radius:6px; background:#0f172a;color:#cbd5e1; cursor:pointer;}
  .btn:hover{background:#111827;}
  .grid { display:grid; grid-template-columns:1fr 1fr; grid-gap:16px; }
  .kpi { display:grid; grid-template-columns:repeat(4,1fr); grid-gap:12px; }
  .kpicard{background:#0b1225;border:1px solid #1f2937;border-radius:10px;padding:10px 12px;}
  .kpt{font-size:12px;color:#93a2b7;margin-bottom:6px;}
  .kpv{font-family:Segoe UI,Roboto,Inter,Helvetica,Arial;font-size:22px;font-weight:700;}
  .chart { height:420px; }
  @media (max-width:1200px){ .grid{grid-template-columns:1fr;} .kpi{grid-template-columns:repeat(2,1fr);} .wrap{grid-template-columns:1fr;} }

  /* --- table readability extras --- */
  .tbl thead th{position:sticky;top:0;background:#111827;border-bottom:1px solid #1f2937;text-align:left;padding:12px 14px;font-weight:800;letter-spacing:.2px;font-size:13px;}
  .tbl tbody td{border-bottom:1px solid #1f2937;padding:10px 14px;vertical-align:top;}
  .tbl tbody tr:nth-child(even) td{background:#0e172a55;}
  .mono{font-family:Consolas,Menlo,Monaco,monospace; font-size:12px; line-height:1.45;}
  .num{color:#93a2b7;font-size:12px;}
  .toastcopy{position:fixed;right:16px;bottom:16px;background:#111827;border:1px solid #374151;color:#e5e7eb;
    padding:8px 12px;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,.35);font:12px Segoe UI,Arial;opacity:0;pointer-events:none;transition:opacity .15s ease}
  .toastcopy.show{opacity:1}
  .sort-arrow{opacity:.7;margin-left:6px}
  .copybtn{
    border:1px solid #1f2937;
    background:#0b1225;
    padding:2px 6px;
    border-radius:6px;
    margin-left:6px;
    cursor:pointer;
    font-size:11px;
    color:#fff;              /* ← белый текст */
  }
  .copybtn:hover{
    background:#111827;
    color:#fff;              /* ← белый и при наведении */
  }
  .preview{max-width:180px;max-height:120px;border:1px solid #1f2937;border-radius:6px;display:block}
</style>
</head>
<body>
  <div class="bar">
    <div class="tabs">
      <button class="tab active" data-tab="analytics">Аналитика</button>
      <button class="tab" data-tab="table">Список пересечений</button>
      <button class="tab" data-tab="dynamic">Динамика</button>
    </div>
    <div class="ts">Сформировано: {ts}</div>
  </div>

  <!-- Новый «красивый» заголовок -->
  <div class="hero">
    <div class="hero-left">
      <div class="hero-title">{xml_title}</div>
      <div class="hero-sub">Отчёт Navisworks • Файл от {xml_date}</div>
    </div>
    <div class="hero-badge">XML</div>
  </div>

  <div class="wrap">
    <div class="panel ctrls">
      <div class="g">
        <div class="h2">Статусы</div>
        <div class="chips" id="statusChips"></div>
      </div>
      <div class="g">
        <div class="h2">Проверяемый раздел</div>
        <div class="chips" id="provSections"></div>
      </div>
      <div class="g">
        <div class="h2">Проверяемая модель</div>
        <div class="chips" id="provModels"></div>
        <div class="btns"><button class="btn" onclick="selectAll('provModels')">Все</button><button class="btn" onclick="clearAll('provModels')">Снять</button><button class="btn" onclick="invertAll('provModels')">Инвертировать</button></div>
      </div>
      <div class="g">
        <div class="h2">Пересекаемый раздел</div>
        <div class="chips" id="intrSections"></div>
      </div>
      <div class="g">
        <div class="h2">Пересекаемая модель</div>
        <div class="chips" id="intrModels"></div>
        <div class="btns"><button class="btn" onclick="selectAll('intrModels')">Все</button><button class="btn" onclick="clearAll('intrModels')">Снять</button><button class="btn" onclick="invertAll('intrModels')">Инвертировать</button></div>
      </div>
      <div class="g muted">Галочки фильтров влияют на все вкладки, графики и аналитику</div>
    </div>

    <div id="analyticsPane">
      <div class="kpi">
        <div class="kpicard"><div class="kpt">Всего коллизий</div><div class="kpv" id="kpiTotal">-</div></div>
        <div class="kpicard"><div class="kpt">Создать</div><div class="kpv" id="kpiCreate">-</div></div>
        <div class="kpicard"><div class="kpt">Активные</div><div class="kpv" id="kpiActive">-</div></div>
        <div class="kpicard"><div class="kpt">Исправленные</div><div class="kpv" id="kpiResolved">-</div></div>
      </div>

      <div class="grid" style="margin-top:16px;">
        <div class="panel pad">
          <h3 class="h">Распределение по статусам</h3>
          <div id="chartStatus" class="chart"></div>
        </div>
        <div class="panel pad">
          <h3 class="h">Коллизии по этажам</h3>
          <div id="chartFloors" class="chart"></div>
        </div>
      </div>

      <div class="panel pad" style="margin-top:16px;">
        <h3 class="h">Коллизии по разделам</h3>
        <div id="chartSections" class="chart"></div>
      </div>

      <div class="panel pad" style="margin-top:16px;">
        <h3 class="h">Популярные пары категорий коллизий</h3>
        <div id="chartPairs" class="chart"></div>
      </div>
    </div>

    <div id="dynamicPane" style="display:none;">
      <div class="panel pad">
        <h3 class="h">Динамика: всего коллизий</h3>
        <div id="dynTotal" class="chart"></div>
      </div>
      <div class="panel pad" style="margin-top:16px;">
        <h3 class="h">Динамика по статусам</h3>
        <div id="dynStatuses" class="chart"></div>
      </div>
      <div class="panel pad" style="margin-top:16px;">
        <h3 class="h">Динамика по разделам</h3>
        <div id="dynSections" class="chart"></div>
      </div>
    </div>

    <div id="tablePane" style="display:none;">
      <div class="tablewrap">
        <div class="muted2">Показываются только записи, попадающие в текущие фильтры (как в графиках).</div>
        <div class="scroll">
          <table class="tbl">
            <thead>
              <tr>
                <th class="nowrap" onclick="setSort('n')">№</th>
                <th class="nowrap" onclick="setSort('preview')">Снимок</th>
                <th class="nowrap" onclick="setSort('cname')">Имя конфликта <span class="muted2">(всего: <span id="clashCount">0</span>)</span></th>
                <th class="nowrap" onclick="setSort('testname')">Имя проверки</th>
                <th class="nowrap" onclick="setSort('cat1')">Категория (проверяемая)</th>
                <th class="nowrap" onclick="setSort('cat2')">Категория (пересекаемая)</th>
                <th class="nowrap" onclick="setSort('id1')">ID (проверяемая)</th>
                <th class="nowrap" onclick="setSort('id2')">ID (пересекаемая)</th>
                <th class="grow" onclick="setSort('path1')">Путь (проверяемая)</th>
                <th class="grow" onclick="setSort('path2')">Путь (пересекаемая)</th>
              </tr>
            </thead>
            <tbody id="clashTableBody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
  <div id="toastCopy" class="toastcopy">Скопировано</div>
<script>
var DATA = {rows: %ROWS%, statusColors: %SCOLORS%, history: %HISTORY%};
</script>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>

<script>
(function(){
  var rows = (DATA && DATA.rows) ? DATA.rows : [];
  var HISTORY = (DATA && DATA.history) ? DATA.history : [];

  function uniq(arr){ return Array.from(new Set(arr)); }
  function by(k){ return function(a,b){ return a[k] > b[k] ? -1 : a[k] < b[k] ? 1 : 0; }; }

  function makeChip(containerId, values, checkedAll, selectedSet){
    var c = document.getElementById(containerId);
    c.innerHTML = '';
    (values||[]).forEach(function(v){
      var lbl = document.createElement('label');
      var inp = document.createElement('input');
      inp.type = 'checkbox';
      inp.value = v;
      inp.checked = selectedSet ? selectedSet.has(v) : (checkedAll !== false);
      inp.addEventListener('change', updateAll);
      inp.addEventListener('input', updateAll);
      var span = document.createElement('span');
      span.textContent = v;
      lbl.appendChild(inp); lbl.appendChild(span);
      c.appendChild(lbl);
    });
  }

  function valuesFrom(containerId){
    return Array.prototype.map.call(
      document.querySelectorAll('#'+containerId+' input:checked'),
      function(x){ return x.value; }
    );
  }
  function selectAll(id){ document.querySelectorAll('#'+id+' input').forEach(function(i){ i.checked = true; }); updateAll(); }
  function clearAll(id){ document.querySelectorAll('#'+id+' input').forEach(function(i){ i.checked = false; }); updateAll(); }
  function invertAll(id){ document.querySelectorAll('#'+id+' input').forEach(function(i){ i.checked = !i.checked; }); updateAll(); }
  window.selectAll = selectAll; window.clearAll = clearAll; window.invertAll = invertAll;

  // ======== Связка "Разделы -> Модели" =========
  var FILE_SECTION = {};
  rows.forEach(function(r){
    if(r.fileA) FILE_SECTION[r.fileA] = r.sectionA || 'Прочее';
    if(r.fileB) FILE_SECTION[r.fileB] = r.sectionB || 'Прочее';
  });
  var allSections = uniq([].concat(rows.map(function(r){return r.sectionA;}), rows.map(function(r){return r.sectionB;}))).filter(function(s){return s;});
  var unionModels = uniq([].concat(rows.map(function(r){return r.fileA;}), rows.map(function(r){return r.fileB;}))).filter(function(s){return s;});

  function modelsForSections(sectionList){
    if(!sectionList || sectionList.length===0) return unionModels.slice();
    return unionModels.filter(function(m){ return sectionList.indexOf(FILE_SECTION[m])>=0; });
  }

  function rebuildModelsUI(){
    var ps = valuesFrom('provSections');
    var isec = valuesFrom('intrSections');
    var provCandidates = modelsForSections(ps);
    var intrCandidates = modelsForSections(isec);
    var prevProvSel = new Set(valuesFrom('provModels'));
    var prevIntrSel = new Set(valuesFrom('intrModels'));
    var keepProv = new Set(provCandidates.filter(function(m){ return prevProvSel.has(m); }));
    var keepIntr = new Set(intrCandidates.filter(function(m){ return prevIntrSel.has(m); }));
    makeChip('provModels', provCandidates, true, keepProv.size ? keepProv : null);
    makeChip('intrModels', intrCandidates, true, keepIntr.size ? keepIntr : null);
  }

  document.addEventListener('change', function(ev){
    var chips = ev.target.closest('.chips');
    if(!chips) return;
    var id = chips.id || '';
    if(id==='provSections' || id==='intrSections'){
      rebuildModelsUI();
      updateAll();
    }
  });

  // ======== Фильтрация данных для графиков/таблицы =========
  function passesByModelsSymmetric(r, pm, im){
    function ok(ma, mb){
      var modOk = (pm.length===0 || pm.indexOf(ma)>=0) && (im.length===0 || im.indexOf(mb)>=0);
      return modOk;
    }
    return ok(r.fileA, r.fileB) || ok(r.fileB, r.fileA);
  }

  function dedup(list){
    var order={'Создать':5,'Активные':4,'Подтвержденные':3,'Проверенные':2,'Исправленные':1};
    var arr=list.slice().sort(function(a,b){
      var da=order[a.status]||0, db=order[b.status]||0;
      if(db!==da) return db-da;
      var ka=a.idsig||a.sig||''; var kb=b.idsig||b.sig||'';
      if(ka<kb) return -1; if(ka>kb) return 1;
      return (a.cname||'').localeCompare(b.cname||'');
    });
    var seen={}; var out=[];
    for(var i=0;i<arr.length;i++){
      var r=arr[i]; var k=(r.idsig&&r.idsig.length>0)?r.idsig:(r.sig||'');
      if(!k){ out.push(r); continue; }
      if(seen[k]) continue; seen[k]=1; out.push(r);
    }
    return out;
  }

  function currentFiltered(){
    var st = valuesFrom('statusChips');
    var pm = valuesFrom('provModels');
    var im = valuesFrom('intrModels');
    return rows.filter(function(r){
      var statusOk = (st.length===0 || st.indexOf(r.status)>=0);
      return statusOk && passesByModelsSymmetric(r, pm, im);
    });
  }

  function currentFilteredFor(rowsInput){
    var st = valuesFrom('statusChips');
    var pm = valuesFrom('provModels');
    var im = valuesFrom('intrModels');
    return rowsInput.filter(function(r){
      var statusOk = (st.length===0 || st.indexOf(r.status)>=0);
      return statusOk && passesByModelsSymmetric(r, pm, im);
    });
  }

  function esc(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  function escAttr(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/"/g,'&quot;'); }

  function resolveAB(r){
    var pm = valuesFrom('provModels');
    var useA = true;
    if(pm && pm.length>0){
      var aMatch = pm.indexOf(r.fileA)>=0;
      var bMatch = pm.indexOf(r.fileB)>=0;
      if(aMatch && !bMatch) useA = true;
      else if(bMatch && !aMatch) useA = false;
    }
    var obj = useA ?
      {cat1:r.catA, cat2:r.catB, id1:r.ida, id2:r.idb, path1:r.t1, path2:r.t2, file1:r.fileA, file2:r.fileB, cname:r.cname, testname:(r.testname||'')} :
      {cat1:r.catB, cat2:r.catA, id1:r.idb, id2:r.ida, path1:r.t2, path2:r.t1, file1:r.fileB, file2:r.fileA, cname:r.cname, testname:(r.testname||'')};
    obj.href = r.href || '';
    return obj;
  }
  var SORT_KEY = null; var SORT_ASC = true;
  function showToast(msg){
    try{
      var t=document.getElementById('toastCopy'); if(!t) return;
      t.textContent = msg || 'Скопировано';
      t.classList.add('show');
      clearTimeout(window.__toastTimer);
      window.__toastTimer = setTimeout(function(){ t.classList.remove('show'); }, 1200);
    }catch(e){}
  }
  function updateSortIcons(){
    try{
      var heads = document.querySelectorAll('#tablePane thead th');
      for(var i=0;i<heads.length;i++){
        var th = heads[i];
        var key = th.getAttribute('onclick'); // like setSort('id1')
        if(!key){ th.innerHTML = th.innerHTML.replace(/\s*[▲▼]$/, ''); continue; }
        var m = key.match(/setSort\('([^']+)'\)/);
        if(!m){ continue; }
        var k = m[1];
        var label = th.textContent.replace(/[▲▼]\s*$/,'').trim();
        if(k===SORT_KEY){
          th.innerHTML = esc(label)+' <span class="sort-arrow">'+(SORT_ASC?'▲':'▼')+'</span>';
        } else {
          th.innerHTML = esc(label);
        }
      }
    }catch(e){}
  }

  function setSort(key){
    if(SORT_KEY===key){ SORT_ASC = !SORT_ASC; } else { SORT_KEY = key; SORT_ASC = true; }
    updateSortIcons();
    renderTable();
  }
  function sortProjected(arr){
    if(!SORT_KEY) return arr;
    var kk = SORT_KEY;
    return arr.slice().sort(function(a,b){
      var A=resolveAB(a), B=resolveAB(b);
      var va=A[kk], vb=B[kk];
      if(kk==='n'){ va=a.__n||0; vb=b.__n||0; }
      if(va==null) va=''; if(vb==null) vb='';
      var na=+va, nb=+vb;
      var numeric = (kk==='id1'||kk==='id2'||kk==='n') && !isNaN(na) && !isNaN(nb);
      if(numeric) return SORT_ASC ? (na-nb) : (nb-na);
      va=(''+va).toLowerCase(); vb=(''+vb).toLowerCase();
      if(va<vb) return SORT_ASC?-1:1;
      if(va>vb) return SORT_ASC?1:-1;
      return 0;
    });
  }
  function copyText(t){
    try{ navigator.clipboard.writeText(String(t||'')); }
    catch(e){ var ta=document.createElement('textarea'); ta.value=String(t||''); document.body.appendChild(ta); ta.select(); try{ document.execCommand('copy'); }catch(e2){} document.body.removeChild(ta); }
  }
  window.setSort = setSort;
  window.copyText = copyText;

  function renderTable(filtered){
    filtered = filtered || currentFiltered();
    filtered = dedup(filtered);
    for(var i=0;i<filtered.length;i++){ filtered[i].__n = i+1; }
    filtered = sortProjected(filtered);
    var tbody = document.getElementById('clashTableBody');
    if(!tbody) return;
    if(filtered.length===0){
      tbody.innerHTML = '<tr><td colspan="10" class="muted2">Нет записей по текущим фильтрам</td></tr>';
      var counter = document.getElementById('clashCount'); if(counter) counter.textContent = 0;
      return;
    }
    var html = [];
    for (var i=0;i<filtered.length;i++){
      var r = filtered[i];
      var p = resolveAB(r);
      var n = i+1;
      var href = (p.href||'').replace(/\\\\/g,'/');
      var src = href ? encodeURI(href) : '';
      var preview = src ? ('<a href="'+src+'" target="_blank"><img class="preview" loading="lazy" src="'+src+'" alt=""></a>') : '';
      html.push('<tr>'
        + '<td class="nowrap">'+n+'</td>'
        + '<td class="nowrap">'+preview+'</td>'
        + '<td class="nowrap">'+esc(p.cname||'')+'</td>'
        + '<td class="nowrap">'+esc(p.testname||'')+'</td>'
        + '<td class="nowrap">'+esc(p.cat1||'')+'</td>'
        + '<td class="nowrap">'+esc(p.cat2||'')+'</td>'
        + '<td class="nowrap"><span class="mono">'+esc(p.id1||'')+'</span> <button class="copybtn" title="Скопировать ID проверяемой модели" data-copy="'+escAttr(String(p.id1||''))+'">copy</button></td>'
        + '<td class="nowrap">'+esc(p.id2||'')+'</td>'
        + '<td class="mono">'+esc(p.path1||'')+'</td>'
        + '<td class="mono">'+esc(p.path2||'')+'</td>'
        + '</tr>');
    }
    tbody.innerHTML = html.join('');
    tbody.querySelectorAll('.copybtn').forEach(function(b){
      b.addEventListener('click', function(ev){
        var val = b.getAttribute('data-copy') || '';
        copyText(val);
        showToast('Скопировано');
        ev.stopPropagation();
      });
    });
    var counter = document.getElementById('clashCount'); if(counter) counter.textContent = filtered.length;
  }

  function activateTab(tab){
    var a = document.querySelector('.tab[data-tab="analytics"]');
    var t = document.querySelector('.tab[data-tab="table"]');
    var d = document.querySelector('.tab[data-tab="dynamic"]');
    if(!a || !t || !d) return;
    a.classList.remove('active'); t.classList.remove('active'); d.classList.remove('active');
    document.getElementById('analyticsPane').style.display='none';
    document.getElementById('tablePane').style.display='none';
    document.getElementById('dynamicPane').style.display='none';
    if(tab==='table'){
      t.classList.add('active');
      document.getElementById('tablePane').style.display='block';
      renderTable();
    } else if(tab==='dynamic'){
      d.classList.add('active');
      document.getElementById('dynamicPane').style.display='block';
      renderDynamic();
      try{ chDynTotal && chDynTotal.resize(); }catch(e){}
      try{ chDynStatuses && chDynStatuses.resize(); }catch(e){}
      try{ chDynSections && chDynSections.resize(); }catch(e){}
      try{ chDynSections && chDynSections.resize(); }catch(e){}
    } else {
      a.classList.add('active');
      document.getElementById('analyticsPane').style.display='block';
    }
  }
  document.addEventListener('click', function(ev){
    var btn = ev.target.closest('.tab');
    if(btn){ activateTab(btn.getAttribute('data-tab')); }
  });

  // Charts init
  var chStatus = echarts.init(document.getElementById('chartStatus'));
  var STC = (DATA && DATA.statusColors) ? DATA.statusColors : {};
  var PALETTE = { floors:'#60A5FA', sections:'#8B5CF6', pairs:'#F59E0B' };
  var chFloors = echarts.init(document.getElementById('chartFloors'));
  var chSections = echarts.init(document.getElementById('chartSections'));
  var chPairs = echarts.init(document.getElementById('chartPairs'));
  var chDynTotal = echarts.init(document.getElementById('dynTotal'));
  var chDynSections = echarts.init(document.getElementById('dynSections'));
  var chDynStatuses = echarts.init(document.getElementById('dynStatuses'));
  function resizeAllCharts(){
    try{ chStatus && chStatus.resize(); }catch(e){}
    try{ chFloors && chFloors.resize(); }catch(e){}
    try{ chSections && chSections.resize(); }catch(e){}
    try{ chPairs && chPairs.resize(); }catch(e){}
    try{ chDynTotal && chDynTotal.resize(); }catch(e){}
    try{ chDynStatuses && chDynStatuses.resize(); }catch(e){}
    try{ chDynSections && chDynSections.resize(); }catch(e){}
  }
  window.addEventListener('resize', resizeAllCharts);

  function renderDynamic(){
    var H = (HISTORY||[]).slice().sort(function(a,b){
      var da = new Date((a.ts||'').replace(' ','T'));
      var db = new Date((b.ts||'').replace(' ','T'));
      return da - db;
    });
    var labels = H.map(function(h){ return h.ts; });
    var totals = H.map(function(h){ return dedup(currentFilteredFor(h.rows)).length; });

    chDynTotal.setOption({ textStyle:{color:'#fff'},
      tooltip:{trigger:'axis'},
      xAxis:{type:'category', data:labels, axisLabel:{color:'#fff'}, axisLine:{lineStyle:{color:'#64748b'}}, splitLine:{lineStyle:{color:'#1f2937'}}},
      yAxis:{type:'value', axisLabel:{color:'#fff'}, axisLine:{lineStyle:{color:'#64748b'}}, splitLine:{lineStyle:{color:'#1f2937'}}},
      series:[{type:'line', data:totals}]
    });

    var allStatuses = ['Создать','Активные','Проверенные','Подтвержденные','Исправленные'];
    var series = allStatuses.map(function(s){
      var data = H.map(function(h){
        var r = dedup(currentFilteredFor(h.rows)).filter(function(x){ return x.status===s; });
        return r.length;
      });
      return { name:s, type:'line', data:data, emphasis:{focus:'series'}, itemStyle:{color:(STC[s]||undefined)} };
    });

    chDynStatuses.setOption({ textStyle:{color:'#fff'},
      legend:{data: allStatuses, textStyle:{color:'#fff'}},
      tooltip:{trigger:'axis'},
      xAxis:{type:'category', data:labels, axisLabel:{color:'#fff'}, axisLine:{lineStyle:{color:'#64748b'}}, splitLine:{lineStyle:{color:'#1f2937'}}},
      yAxis:{type:'value', axisLabel:{color:'#fff'}, axisLine:{lineStyle:{color:'#64748b'}}, splitLine:{lineStyle:{color:'#1f2937'}}},
      series: series
    });
    
    // ---- Динамика по разделам ----
    (function(){
      var pm = valuesFrom('provModels');
      var pmSet = {}; pm.forEach(function(x){ pmSet[x]=1; });
      function rowHasPM(r){ return !!pmSet[r.fileA] || !!pmSet[r.fileB]; }

      // helper: rows of one snapshot -> filtered + deduped (+ filtered by PM if задана)
      function snapRows(h){
        var uniq = dedup(currentFilteredFor(h.rows));
        if(pm && pm.length>0) uniq = uniq.filter(rowHasPM);
        return uniq;
      }

      // Собираем список разделов, которые относятся к выбранной "Проверяемой модели" (если выбрана)
      var limitSectionsSet = null;
      if(pm && pm.length>0){
        limitSectionsSet = {};
        for (var i=0;i<H.length;i++){
          var rows = snapRows(H[i]);
          rows.forEach(function(r){
            if(pmSet[r.fileA] && r.sectionA) limitSectionsSet[r.sectionA]=1;
            if(pmSet[r.fileB] && r.sectionB) limitSectionsSet[r.sectionB]=1;
          });
        }
      }

      // Формируем полный список разделов, попавших в выборку (и соответствующих PM, если она выбрана)
      var allSecSet = {};
      for (var i=0;i<H.length;i++){
        var rows = snapRows(H[i]);
        rows.forEach(function(r){
          var a = r.sectionA || '', b = r.sectionB || '';
          if(!a && !b) return;
          if(limitSectionsSet){
            if(a && limitSectionsSet[a]) allSecSet[a]=1;
            if(b && limitSectionsSet[b]) allSecSet[b]=1;
          }else{
            if(a) allSecSet[a]=1;
            if(b) allSecSet[b]=1;
          }
        });
      }
      var allSectionsDyn = Object.keys(allSecSet).filter(function(x){return x;});

      // Серии: симметричный подсчёт, но self-self считаем 1 раз
      var secSeries = allSectionsDyn.map(function(secName){
        var data = H.map(function(h){
          var rows = snapRows(h);
          var c = 0;
          for (var k=0;k<rows.length;k++){
            var a = rows[k].sectionA||'';
            var b = rows[k].sectionB||'';
            if(a===b){
              if(a===secName) c += 1;
            } else {
              if(a===secName) c += 1;
              if(b===secName) c += 1;
            }
          }
          return c;
        });
        return { 
          name: secName, 
          type: 'line', 
          stack: 'total',
          areaStyle: {}, 
          emphasis:{focus:'series'},
          data: data 
        };
      });

      try{ chDynSections.clear(); }catch(e){}
      chDynSections.setOption({
        textStyle:{color:'#fff'},
        legend:{data: allSectionsDyn, textStyle:{color:'#fff'}},
        tooltip:{trigger:'axis'},
        xAxis:{type:'category', data: labels, axisLabel:{color:'#fff'}, axisLine:{lineStyle:{color:'#64748b'}}, splitLine:{lineStyle:{color:'#1f2937'}}},
        yAxis:{type:'value', axisLabel:{color:'#fff'}, axisLine:{lineStyle:{color:'#64748b'}}, splitLine:{lineStyle:{color:'#1f2937'}}},
        series: secSeries
      });
    })();
;
;
;
;
}

  function swapRow(r){
    return {
      status: r.status,
      fileA: r.fileB, fileB: r.fileA,
      sectionA: r.sectionB, sectionB: r.sectionA,
      floorA: r.floorB, floorB: r.floorA,
      paircats: r.paircats, sig: r.sig,
      t1: r.t2, t2: r.t1, cname: r.cname,
      ida: r.idb, idb: r.ida, idsig: r.idsig,
      catA: r.catB, catB: r.catA,
      href: r.href
    };
  }
  function orientedRows(uniqRows){
    var pm = valuesFrom('provModels');
    var im = valuesFrom('intrModels');
    var pmSet = {}; pm.forEach(function(x){ pmSet[x]=1; });
    var imSet = {}; im.forEach(function(x){ imSet[x]=1; });
    function needSwap(r){
      if(pm.length>0){
        if(pmSet[r.fileA]) return false;
        if(pmSet[r.fileB]) return true;
      }
      if(pm.length===0 && im.length>0){
        if(imSet[r.fileA] && !imSet[r.fileB]) return true;
        return false;
      }
      var a = (r.fileA||''), b=(r.fileB||'');
      return b < a;
    }
    var out = [];
    for(var i=0;i<uniqRows.length;i++){
      var r = uniqRows[i];
      out.push(needSwap(r) ? swapRow(r) : r);
    }
    return out;
  }

  function updateAll(){
    var filtered = currentFiltered();
    var uniqRows = dedup(filtered);

    // KPI
    var el;
    if ((el=document.getElementById('kpiTotal')))    el.textContent = uniqRows.length;
    if ((el=document.getElementById('kpiCreate')))   el.textContent = uniqRows.filter(function(r){return r.status==='Создать';}).length;
    if ((el=document.getElementById('kpiActive')))   el.textContent = uniqRows.filter(function(r){return r.status==='Активные';}).length;
    if ((el=document.getElementById('kpiResolved'))) el.textContent = uniqRows.filter(function(r){return r.status==='Исправленные';}).length;

    // Статусы
    var sc = {};
    uniqRows.forEach(function(r){ sc[r.status]=(sc[r.status]||0)+1; });
    var sLabels = ['Создать','Активные','Проверенные','Подтвержденные','Исправленные'];
    chStatus.setOption({ tooltip:{trigger:'axis'},
      xAxis:{ type:'category', data:sLabels }, yAxis:{ type:'value' },
      series:[{ type:'bar', data:sLabels.map(function(s){ return {value:(sc[s]||0), itemStyle:{color:(STC[s]||'#999')}}; }) }]
    });

    // Этажи
    var fl = {};
    var oRows = orientedRows(uniqRows);
    oRows.forEach(function(r){
      var fa = (r.floorA||'').trim();
      if(fa) fl[fa]=(fl[fa]||0)+1;
    });
    var flLabels = Object.keys(fl).sort();
    chFloors.setOption({ tooltip:{trigger:'axis'},
      xAxis:{ type:'category', data:flLabels }, yAxis:{ type:'value' },
      series:[{ type:'bar', data:flLabels.map(function(s){return fl[s]||0;}), itemStyle:{ color: '#60A5FA' } }]
    });

    // Разделы
    
    // Разделы (симметрично: +1 обоим разделам у каждой пары)
    
    // Разделы (симметрично, но self-self считаем 1 раз)
    var secC = {};
    uniqRows.forEach(function(r){
      var sa = r.sectionA, sb = r.sectionB;
      if(sa && sb && sa === sb){
        secC[sa] = (secC[sa]||0) + 1;
      } else {
        if(sa) secC[sa] = (secC[sa]||0) + 1;
        if(sb) secC[sb] = (secC[sb]||0) + 1;
      }
    });
    var secLabels = Object.keys(secC).filter(function(x){return x;}).sort();
    chSections.setOption({ tooltip:{trigger:'axis'},
      xAxis:{ type:'category', data:secLabels }, yAxis:{ type:'value' },
      series:[{ type:'bar', data:secLabels.map(function(s){return secC[s]||0;}), itemStyle:{ color: '#8B5CF6' } }]
    });



    
    // Пары категорий (все пары; A—B и B—A объединяем; self-self = 1 раз)
    var pairsEl = document.getElementById('chartPairs');
    var pc = {};
    uniqRows.forEach(function(r){
      var a = (r.catA||'').trim();
      var b = (r.catB||'').trim();
      if(!a || !b) return; // пары только при обеих категориях
      var A = a<=b ? a : b;
      var B = a<=b ? b : a;
      var key = A + ' — ' + B;
      pc[key] = (pc[key]||0) + 1; // self-self автоматически +1
    });
    var pairLabels = Object.keys(pc).sort(function(x,y){
      // сортировка по убыванию значения, затем по алфавиту
      var dx = pc[y]-pc[x]; if(dx!==0) return dx;
      return x.localeCompare(y);
    });

    // Динамическая высота: 28px на строку + отступы
    try{
      var h = Math.max(240, 28 * pairLabels.length + 40);
      if(pairsEl && pairsEl.style) { pairsEl.style.height = h + 'px'; }
      if(chPairs && chPairs.resize) chPairs.resize();
    }catch(e){}

    chPairs.setOption({
      textStyle:{color:'#fff'},
      tooltip:{trigger:'axis'},
      grid:{left: 320, right: 16, top: 16, bottom: 16, containLabel: true},
      xAxis:{ 
        type:'value',
        axisLabel:{color:'#fff'},
        axisLine:{lineStyle:{color:'#64748b'}},
        splitLine:{lineStyle:{color:'#1f2937'}}
      },
      yAxis:{ 
        type:'category',
        data: pairLabels,
        axisLabel:{
          color:'#fff',
          interval: 0,
          width: 300,
          overflow: 'break',
          lineHeight: 14
        },
        axisLine:{lineStyle:{color:'#64748b'}},
        splitLine:{show:false}
      },
      series:[{
        type:'bar',
        data: pairLabels.map(function(k){return pc[k]||0;}),
        itemStyle:{ color: '#F59E0B' },
        label:{
          show:true,
          position:'right',
          color:'#fff',
          formatter: function(p){ return p.value; }
        }
      }]
    });


    if (document.getElementById('tablePane') && document.getElementById('tablePane').style.display!=='none'){ renderTable(uniqRows); }
    if (document.getElementById('dynamicPane') && document.getElementById('dynamicPane').style.display!=='none'){ renderDynamic(); }
  }

  document.addEventListener('change', function(ev){
    var chips = ev.target.closest('.chips');
    if(!chips) return;
    var id = chips.id || '';
    if(id!=='provSections' && id!=='intrSections'){
      updateAll();
    }
  });

  makeChip('statusChips', ['Создать','Активные','Проверенные','Подтвержденные','Исправленные'], true);
  makeChip('provSections', allSections, true);
  makeChip('intrSections', allSections, true);
  rebuildModelsUI();
  updateAll();

  var active = document.querySelector('.tab.active');
  if(active && active.getAttribute('data-tab')==='table'){ renderTable(); }
  if(active && active.getAttribute('data-tab')==='dynamic'){ renderDynamic(); }
})();
</script>

</body>
</html>
"""

def build_html(xml_path, rows, history):
    # Дата формирования HTML
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    # Название XML и его дата (mtime)
    xml_title = os.path.basename(xml_path)
    try:
        xml_mtime = os.path.getmtime(xml_path)
        xml_date = datetime.datetime.fromtimestamp(xml_mtime).strftime('%Y-%m-%d %H:%M')
    except Exception:
        xml_date = u'—'

    html = HTML_TEMPLATE.replace('{ts}', ts)\
                        .replace('{xml_title}', xml_title)\
                        .replace('{xml_date}', xml_date)
    html = html.replace('%ROWS%', json.dumps(rows, ensure_ascii=False))
    html = html.replace('%SCOLORS%', json.dumps(STATUS_COLORS, ensure_ascii=False))
    html = html.replace('%HISTORY%', json.dumps(history, ensure_ascii=False))
    outdir = os.path.dirname(xml_path)
    outname = os.path.splitext(os.path.basename(xml_path))[0] + u'_report.html'
    outpath = os.path.join(outdir, outname)
    with io.open(outpath, 'w', encoding='utf-8') as f:
        f.write(html)
    return outpath

def pick_xml():
    if forms:
        return forms.pick_file(file_ext='xml', title='Выберите XML отчёт Navisworks')
    return None

def main():
    xml_path = pick_xml()
    if not xml_path:
        raise Exception(u'Файл XML не выбран.')

    try:
        rows = parse_xml(xml_path)
    except Exception as ex:
        msg = u'Не удалось разобрать XML.\n{0}'.format(ex)
        if forms: forms.alert(msg, title=u'HTML отчёт')
        else: print(msg)
        return

    if not rows:
        if forms: forms.alert(u'В отчёте нет результатов (или не распознан формат).', title=u'HTML отчёт')
        else: print('No rows parsed')
        return

    # базовая дата = mtime выбранного отчёта
    try:
        _mt = os.path.getmtime(xml_path)
        _ref_dt = datetime.datetime.fromtimestamp(_mt)
    except Exception:
        _ref_dt = datetime.datetime.now()
    since_dt = _ask_since_select(_ref_dt)
    history = find_history_reports(xml_path, limit=5, since_dt=since_dt)
    if since_dt is not None and not history and forms:
        forms.alert(u'По выбранному периоду исторических отчётов не найдено.', title=u'Динамика')
    outpath = build_html(xml_path, rows, history)
    if script:
        script.open_url(outpath)
    else:
        print('Saved:', outpath)

if __name__ == '__main__':
    main()
