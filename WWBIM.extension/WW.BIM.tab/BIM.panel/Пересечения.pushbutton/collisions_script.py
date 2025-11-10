# -*- coding: utf-8 -*-
__title__  = u"Пересечения"
__author__ = u"vlad / you"
__doc__    = u"Читает XML-отчёт Navisworks (в т.ч. «Все тесты…»), показывает все проверки (даже пустые), фильтрует по текущей модели Revit, печатает таблицы с подсветкой категории, выводит сводку и дату отчёта."

import os
import re
from datetime import datetime
from pyrevit import forms, script
from Autodesk.Revit.DB import ElementId

# -------- Настройки доп. фильтрации --------
FILTER_BY_FILE     = True   # сверка файла из pathlink с текущей моделью
FILTER_BY_CATEGORY = False  # при необходимости дополнительно сверять категорию в тексте pathlink

# ---------- UI ----------
out = script.get_output()
out.close_others(all_open_outputs=True)
out.set_width(1200)

uidoc = __revit__.ActiveUIDocument
doc   = uidoc.Document

# ---------- Вспомогательное ----------
_EL_CACHE = {}   # id (int) -> (element_or_None, category_name_or_u"")

def _get_el_and_cat(eid_int):
    if eid_int in _EL_CACHE:
        return _EL_CACHE[eid_int]
    try:
        el = doc.GetElement(ElementId(eid_int))
        cat = (el.Category.Name if el and el.Category else u"") or u""
    except Exception:
        el, cat = None, u""
    _EL_CACHE[eid_int] = (el, cat)
    return el, cat

def _norm_key(s):
    if not s:
        return u""
    return re.sub(u'[^0-9a-zA-Zа-яА-Я]+', u'', s, flags=re.UNICODE).lower()

DOC_TITLE_KEY = _norm_key(doc.Title)

# Соберём перечень всех категорий из документа (для подсветки по тексту пути)
_DOC_CAT_KEYS = set()
try:
    for c in doc.Settings.Categories:
        try:
            nm = c.Name
            if nm:
                _DOC_CAT_KEYS.add(_norm_key(nm))
        except Exception:
            pass
except Exception:
    pass

# ---------- XML backend ----------
try:
    from lxml import etree as ET
except Exception:
    import xml.etree.ElementTree as ET

# =================== устойчивое чтение XML ===================

_BAD_AMP_RE = re.compile(u'&(?!amp;|lt;|gt;|apos;|quot;|#\\d+;|#x[0-9A-Fa-f]+;)', re.UNICODE)

def _read_text_guess_enc(path):
    with open(path, 'rb') as f:
        data = f.read()
    for enc in ('utf-8-sig', 'utf-16', 'utf-8', 'cp1251'):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode('utf-8', 'ignore')

def _strip_invalid_xml_chars(s):
    out_chars = []
    for ch in s:
        c = ord(ch)
        if (c == 0x9 or c == 0xA or c == 0xD or
            (0x20 <= c <= 0xD7FF) or
            (0xE000 <= c <= 0xFFFD) or
            (0x10000 <= c <= 0x10FFFF)):
            out_chars.append(ch)
        else:
            out_chars.append(u' ')
    return u''.join(out_chars)

def _sanitize_xml_text(s):
    s = s.replace(u'\r\n', u'\n').replace(u'\r', u'\n')
    s = s.replace(u'\ufeff', u' ')
    s = _strip_invalid_xml_chars(s)
    s = _BAD_AMP_RE.sub(u'&amp;', s)
    p = s.find(u'<')
    if p > 0:
        s = s[p:]
    return s

def _safe_xml_root(xml_path):
    raw = _read_text_guess_enc(xml_path)
    cleaned = _sanitize_xml_text(raw)
    return ET.fromstring(cleaned)

# =================== общее ===================

def _img_abs_from_href(base_dir, href_rel):
    if not href_rel:
        return u''
    href_rel = (href_rel or u'').replace('\\', '/').lstrip('./')
    return os.path.normpath(os.path.join(base_dir, href_rel))

def _pathlink_string_from_element(co):
    parts = []
    for node in co.findall('./pathlink/node'):
        t = (node.text or '').strip()
        if t:
            parts.append(t)
    if not parts:
        for node in co.findall('./pathlink/path'):
            t = (node.text or '').strip()
            if t:
                parts.append(t)
    return u' / '.join(parts)

def _object_id_from_element(co):
    for st in co.findall('./smarttags/smarttag'):
        name = (st.findtext('name') or '').strip().lower()
        if name in ('объект id', 'object id', 'object 1 id', 'object 2 id', 'id объекта'):
            val = (st.findtext('value') or '').strip()
            if val.isdigit():
                return int(val)
    for oa in co.findall('./objectattribute'):
        name = (oa.findtext('name') or '').strip().lower()
        if name in ('id объекта', 'object id', 'объект id'):
            val = (oa.findtext('value') or '').strip()
            if val.isdigit():
                return int(val)
    return None

_FILE_RX = re.compile(u'.+\\.(nwc|nwd|nwf|rvt)$', re.IGNORECASE)
def _path_first_filename(path_text):
    if not path_text:
        return u''
    tokens = [t.strip() for t in path_text.split(u'/')]
    for t in tokens:
        t = t.strip()
        if _FILE_RX.match(t):
            return os.path.splitext(t)[0]
    return u''

def _filename_matches_current_model(path_text):
    base = _path_first_filename(path_text)
    if not base:
        return True
    return _norm_key(base) in DOC_TITLE_KEY or DOC_TITLE_KEY in _norm_key(base)

# ========= извлечение даты отчёта =========

_DATE_ATTRS = ('date','created','generated','exported','timestamp','time','start','lastsaved','creationtime','modificationtime')
_RX_DATE_ATTR = re.compile(r'\b(?:date|created|generated|export(?:date|ed)?|timestamp|time|start|lastsaved|creationtime|modificationtime)\s*=\s*"([^"]+)"', re.IGNORECASE)

def _extract_report_datetime(xml_path):
    try:
        root = _safe_xml_root(xml_path)
        for a in _DATE_ATTRS:
            v = root.get(a)
            if v:
                return v, u"из XML"
        for tag in ('report','batchtest','tests'):
            for el in root.findall('.//{}'.format(tag)):
                for a in _DATE_ATTRS:
                    v = el.get(a)
                    if v:
                        return v, u"из XML"
    except Exception:
        pass
    try:
        txt = _sanitize_xml_text(_read_text_guess_enc(xml_path))
        m = _RX_DATE_ATTR.search(txt)
        if m:
            return m.group(1).strip(), u"из XML"
    except Exception:
        pass
    try:
        ts = os.path.getmtime(xml_path)
        dt = datetime.fromtimestamp(ts)
        return dt.strftime(u"%d.%m.%Y %H:%M"), u"по дате изменения файла"
    except Exception:
        return u"", u""

# =================== нормальный парсер: группировка по проверкам ===================

def _parse_groups_normal(xml_path):
    """
    Возвращает dict: test_name -> [rows];
    row = {'name','img','id','id_other','path','path_other'}.
    Включает тесты без конфликтов (пустые списки).
    """
    root = _safe_xml_root(xml_path)
    base_dir = os.path.dirname(xml_path)

    # Все названия проверок
    all_tests = set()
    for ct in root.findall('.//clashtest'):
        nm = ct.get('name') or ct.get('displayname')
        if nm:
            all_tests.add(nm)
    for t in root.findall('.//test'):
        nm = t.get('name') or t.get('displayname')
        if nm:
            all_tests.add(nm)

    groups = {nm: [] for nm in all_tests}

    # карта ребёнок->родитель
    parent = {}
    for p in root.iter():
        for ch in list(p):
            parent[ch] = p

    def _test_name_for_cr(cr):
        tn = cr.get('testname') or cr.get('test') or cr.get('groupname')
        if tn:
            return tn
        p = parent.get(cr)
        while p is not None:
            tn = p.get('name') or p.get('displayname') or p.get('testname')
            if tn:
                return tn
            p = parent.get(p)
        return u'(Без названия проверки)'

    for cr in root.findall('.//clashresult'):
        test_name  = _test_name_for_cr(cr)
        clash_name = cr.get('name') or u'Без имени'
        img_abs    = _img_abs_from_href(base_dir, cr.get('href') or u'')

        objs = []
        for co in cr.findall('./clashobjects/clashobject'):
            rid = _object_id_from_element(co)
            if rid is None:
                continue
            objs.append({'id': rid, 'path': _pathlink_string_from_element(co)})

        if not objs:
            continue

        rows = groups.setdefault(test_name, [])
        if len(objs) >= 2:
            a, b = objs[0], objs[1]
            rows.append({'name': clash_name, 'img': img_abs, 'id': a['id'], 'id_other': b['id'],
                         'path': a['path'], 'path_other': b['path']})
            rows.append({'name': clash_name, 'img': img_abs, 'id': b['id'], 'id_other': a['id'],
                         'path': b['path'], 'path_other': a['path']})
        else:
            ob = objs[0]
            rows.append({'name': clash_name, 'img': img_abs, 'id': ob['id'], 'id_other': None,
                         'path': ob['path'], 'path_other': u''})

    return groups

# =================== fallback (регулярки) ===================
_RX_CTEST_BLOCK = re.compile(r'<clashtest\b[^>]*>.*?</clashtest>', re.DOTALL | re.IGNORECASE)
_RX_TEST_BLOCK  = re.compile(r'<test\b[^>]*>.*?</test>', re.DOTALL | re.IGNORECASE)
_RX_TEST_NAME   = re.compile(r'\bname=\"([^\"]+)\"', re.IGNORECASE)
_RX_ATTR_TESTNM = re.compile(r'\btestname=\"([^\"]+)\"', re.IGNORECASE)
_RX_CR_BLOCK    = re.compile(r'<clashresult\b[^>]*>.*?</clashresult>', re.DOTALL | re.IGNORECASE)
_RX_ATTR_HREF   = re.compile(r'\bhref=\"([^\"]*)\"', re.IGNORECASE)
_RX_ATTR_CNAME  = re.compile(r'\bname=\"([^\"]+)\"', re.IGNORECASE)
_RX_CO_BLOCK    = re.compile(r'<clashobject\b[^>]*>.*?</clashobject>', re.DOTALL | re.IGNORECASE)
_RX_SMART_PAIR  = re.compile(r'<smarttag>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</smarttag>', re.DOTALL | re.IGNORECASE)
_RX_OA_PAIR     = re.compile(r'<objectattribute>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</objectattribute>', re.DOTALL | re.IGNORECASE)
_RX_PATHLINK    = re.compile(r'<pathlink>.*?</pathlink>', re.DOTALL | re.IGNORECASE)
_RX_NODE_TEXT   = re.compile(r'<node>(.*?)</node>', re.DOTALL | re.IGNORECASE)
_RX_TAGS        = re.compile(r'<[^>]+>')

def _parse_groups_fallback(xml_path):
    """
    Возвращает dict как normal; дополнительно «предзаводит» пустые тесты.
    """
    txt = _sanitize_xml_text(_read_text_guess_enc(xml_path))
    base_dir = os.path.dirname(xml_path)
    groups = {}

    blocks = list(_RX_CTEST_BLOCK.finditer(txt))
    if not blocks:
        blocks = list(_RX_TEST_BLOCK.finditer(txt))
    if not blocks:
        blocks = [type('M', (), {'group': (lambda self, n=0: txt)})()]

    for bm in blocks:
        tblock = bm.group(0) if hasattr(bm, 'group') else bm
        tname_m = _RX_TEST_NAME.search(tblock)
        default_tname = tname_m.group(1) if tname_m else u'(Без названия проверки)'
        groups.setdefault(default_tname, [])  # пустая группа, если конфликтов нет

        for crm in _RX_CR_BLOCK.finditer(tblock):
            crblock = crm.group(0)
            tnm_m = _RX_ATTR_TESTNM.search(crblock)
            test_name = tnm_m.group(1) if tnm_m else default_tname

            img_rel = (_RX_ATTR_HREF.search(crblock).group(1) if _RX_ATTR_HREF.search(crblock) else u'')
            img_abs = _img_abs_from_href(base_dir, img_rel)
            cname   = (_RX_ATTR_CNAME.search(crblock).group(1) if _RX_ATTR_CNAME.search(crblock) else u'Без имени')

            objs = []
            for com in _RX_CO_BLOCK.finditer(crblock):
                coblock = com.group(0)
                rid = None
                for nm, val in _RX_SMART_PAIR.findall(coblock):
                    if (nm or u'').strip().lower() in ('объект id','object id','object 1 id','object 2 id','id объекта'):
                        v = (val or u'').strip()
                        if v.isdigit():
                            rid = int(v); break
                if rid is None:
                    for nm, val in _RX_OA_PAIR.findall(coblock):
                        if (nm or u'').strip().lower() in ('id объекта','object id','объект id'):
                            v = (val or u'').strip()
                            if v.isdigit():
                                rid = int(v); break
                path = u''
                pl = _RX_PATHLINK.search(coblock)
                if pl:
                    parts = [t.strip() for t in _RX_NODE_TEXT.findall(pl.group(0))]
                    parts = [_RX_TAGS.sub(u'', t) for t in parts if t]
                    path  = u' / '.join(parts)
                if rid is not None:
                    objs.append({'id': rid, 'path': path})

            if not objs:
                continue

            rows = groups.setdefault(test_name, [])
            if len(objs) >= 2:
                a, b = objs[0], objs[1]
                rows.append({'name': cname, 'img': img_abs, 'id': a['id'], 'id_other': b['id'],
                             'path': a['path'], 'path_other': b['path']})
                rows.append({'name': cname, 'img': img_abs, 'id': b['id'], 'id_other': a['id'],
                             'path': b['path'], 'path_other': a['path']})
            else:
                ob = objs[0]
                rows.append({'name': cname, 'img': img_abs, 'id': ob['id'], 'id_other': None,
                             'path': ob['path'], 'path_other': u''})

    return groups

# =================== фильтрация, подсветка и статистика ===================

BADGE_STYLE_DARK = u'background:#2f3b4a; color:#fff; padding:1px 6px; border-radius:3px; font-weight:600;'

def _filter_and_annotate(items):
    """Возвращает список строк, прошедших фильтры, с полями cat и cat_other (если возможно)."""
    kept = []
    for it in items:
        try:
            el, cat = _get_el_and_cat(int(it['id']))
            if not el:
                continue

            if FILTER_BY_FILE:
                if not (_filename_matches_current_model(it.get('path', u'')) or
                        _filename_matches_current_model(it.get('path_other', u''))):
                    continue

            if FILTER_BY_CATEGORY and cat:
                ck = cat.strip().lower()
                p1 = (it.get('path', u'') or u'').lower()
                p2 = (it.get('path_other', u'') or u'').lower()
                if (ck not in p1) and (ck not in p2):
                    continue

            it2 = dict(it)
            it2['cat'] = cat

            id_other = it.get('id_other')
            if id_other:
                el2, cat2 = _get_el_and_cat(int(id_other))
                it2['cat_other'] = cat2 if el2 else u""
            else:
                it2['cat_other'] = u""

            kept.append(it2)
        except Exception:
            pass
    return kept

def _first_doc_category_in_segment(seg_norm):
    """Возвращает имя категории (нормализ.) из списка категорий документа, если сегмент похож на неё."""
    if not seg_norm:
        return None
    for ck in _DOC_CAT_KEYS:
        if not ck:
            continue
        # строгая/включающая проверка
        if ck in seg_norm or seg_norm in ck:
            # избегаем слишком общих коротких совпадений
            if len(ck) >= 4 and len(seg_norm) >= 4:
                return ck
    return None

def _hilite_category_in_path(path_text, category_name):
    """
    Подсвечивает категорию в path:
    1) сначала пытаемся по фактическому имени категории (category_name),
    2) если не нашли — ищем по совпадению с перечнем категорий документа.
    """
    if not path_text:
        return u'—'

    segs = [s.strip() for s in path_text.split(u'/')]
    already = False
    ck_name = (category_name or u'').strip()
    ck_norm = _norm_key(ck_name)

    # 1) точное имя категории
    if ck_norm:
        for i, s in enumerate(segs):
            if ck_norm in _norm_key(s):
                segs[i] = u'<span style="{st}">{txt}</span>'.format(st=BADGE_STYLE_DARK, txt=s)
                already = True
                break

    # 2) по списку категорий документа (если в (1) ничего не нашли)
    if not already:
        # отталкиваемся от сегмента после имени файла .nwc/.rvt
        start = 0
        for idx, s in enumerate(segs):
            if _FILE_RX.match(s):
                start = idx + 1
                break
        for i in range(start, len(segs)):
            s = segs[i]
            if '<span' in s:   # уже подсветили
                continue
            seg_norm = _norm_key(s)
            match = _first_doc_category_in_segment(seg_norm)
            if match:
                segs[i] = u'<span style="{st}">{txt}</span>'.format(st=BADGE_STYLE_DARK, txt=s)
                break

    return u' / '.join(segs)

def _build_filtered_groups(groups):
    return {t: _filter_and_annotate(rows) for t, rows in groups.items()}

def _build_stats(filtered_groups):
    stats = {}
    for test, rows in filtered_groups.items():
        seen_pairs = set()
        catpairs   = set()
        for it in rows:
            i1 = it.get('id'); i2 = it.get('id_other')
            if not i1 or not i2:
                continue
            try:
                a, b = int(i1), int(i2)
            except Exception:
                continue
            key = (a, b) if a < b else (b, a)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            c1 = (it.get('cat') or u"").strip()
            c2 = (it.get('cat_other') or u"").strip()
            if not c2:
                # если вторую категорию не удалось получить из Revit — попробуем из текста правого path
                guess = _hilite_category_in_path(it.get('path_other') or u'', u'')
                # вытащим голый текст без html, чтобы отобразить в сводке
                c2 = re.sub(u'<[^>]*>', u'', guess)
                # возьмём сам сегмент (последний <span>…</span>), если есть
                m = re.search(u'<span[^>]*>([^<]+)</span>', guess)
                if m:
                    c2 = m.group(1)

            if c1 or c2:
                catpairs.add(tuple(sorted([c1 or u'—', c2 or u'—'])))

        stats[test] = {'pairs': len(seen_pairs), 'catpairs': catpairs}
    return stats

# =================== печать ===================

def _img_cell(path, w=96):
    if not path or not os.path.exists(path):
        return u'—'
    uri = u'file:///' + path.replace('\\', '/')
    return u'<img src="{0}" width="{1}" />'.format(uri, int(w))

def _print_group(title, rows):
    out.print_md(u"\n---\n### Проверка: **{}**  _(строк: {})_".format(title, len(rows)))
    if not rows:
        out.print_md(u"—")
        return
    table_data = []
    for i, it in enumerate(rows, 1):
        path1 = _hilite_category_in_path(it.get('path') or u'', it.get('cat') or u'')
        path2 = _hilite_category_in_path(it.get('path_other') or u'', it.get('cat_other') or u'')
        table_data.append([
            i,
            _img_cell(it.get('img'), w=96),
            title,
            it.get('name') or u'',
            out.linkify(ElementId(int(it['id']))),
            path1 or u'—',
            path2 or u'—'
        ])
    out.print_table(
        table_data=table_data,
        columns=[u'№', u'Снимок', u'Название проверки', u'Пересечение', u'ID', u'Путь элемента', u'Путь второго элемента'],
        title=None
    )

# =================== MAIN ===================

def main():
    xml_path = forms.pick_file(files_filter="XML (*.xml)|*.xml",
                               title=u"Выберите XML отчёт Navisworks")
    if not xml_path:
        forms.alert(u"Файл не выбран.", title=u"Пересечения")
        return

    try:
        groups = _parse_groups_normal(xml_path)
    except Exception as e:
        out.print_md(u":warning: Нормальный парсер не справился (`{}`). Перехожу на fallback…".format(e))
        groups = _parse_groups_fallback(xml_path)

    if not groups:
        forms.alert(u"Не удалось извлечь данные из отчёта.", title=u"Пересечения")
        return

    # ---- Выбор проверок (включая пустые) ----
    all_tests = sorted(groups.keys(), key=lambda s: s.lower())
    picked = forms.SelectFromList.show(
        all_tests,
        multiselect=True,
        title=u"Выберите проверки (можно несколько). Вверху есть строка поиска.",
        button_name=u"Показать"
    )
    if picked and len(picked) < len(all_tests):
        groups = {t: groups.get(t, []) for t in picked}

    # Дата отчёта
    rep_dt, rep_src = _extract_report_datetime(xml_path)
    if rep_dt:
        out.print_md(u"**Дата отчёта Navisworks:** {} _({})_".format(rep_dt, rep_src))

    # Фильтрация и аннотация
    filtered_groups = _build_filtered_groups(groups)

    # Сводка по коллизиям (по парам элементов, без задвоения)
    stats = _build_stats(filtered_groups)
    out.print_md(u"### Краткая сводка по выбранным проверкам")
    lines = []
    for test in sorted(filtered_groups.keys(), key=lambda s: s.lower()):
        st = stats.get(test, {'pairs': 0, 'catpairs': set()})
        cpairs = u'; '.join(u'{} × {}'.format(a, b) for (a, b) in sorted(st['catpairs']))
        lines.append(u"- **{}** — {} коллизий; пары категорий: {}".format(test, st['pairs'], cpairs or u'—'))
    out.print_md(u'\n'.join(lines))

    total_rows = sum(len(v) for v in filtered_groups.values())
    out.print_md(u"## Пересечения: {} строк ({} проверок)".format(total_rows, len(filtered_groups)))

    for test_name in sorted(filtered_groups.keys(), key=lambda s: s.lower()):
        _print_group(test_name, filtered_groups[test_name])

    out.print_md(u"_Клик по **ID** выделяет элемент. Можно кликать подряд._")

if __name__ == "__main__":
    main()
