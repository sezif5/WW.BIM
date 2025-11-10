# -*- coding: utf-8 -*-
__title__  = u"Пересечения"
__author__ = u"vlad / you"
__doc__    = u"Читает XML-отчёт Navisworks (в т.ч. «Все тесты (совм.)»), группирует по названию проверки и печатает таблицы с заголовками. Устойчив к «грязному» XML."

import os
import re
from pyrevit import forms, script
from Autodesk.Revit.DB import ElementId

# ---------- UI ----------
out = script.get_output()
out.close_others(all_open_outputs=True)
out.set_width(1200)

uidoc = __revit__.ActiveUIDocument
doc   = uidoc.Document

# ---------- XML backend: lxml если есть, иначе стандартный ET ----------
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
    """Оставляем только допустимые XML 1.0: \\t \\n \\r и U+0020.., без C0/C1."""
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
    p = s.find(u'<')    # отрезаем мусор до первого тега
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
    # smarttags
    for st in co.findall('./smarttags/smarttag'):
        name = (st.findtext('name') or '').strip().lower()
        if name in ('объект id', 'object id', 'object 1 id', 'object 2 id', 'id объекта'):
            val = (st.findtext('value') or '').strip()
            if val.isdigit():
                return int(val)
    # objectattribute
    for oa in co.findall('./objectattribute'):
        name = (oa.findtext('name') or '').strip().lower()
        if name in ('id объекта', 'object id', 'объект id'):
            val = (oa.findtext('value') or '').strip()
            if val.isdigit():
                return int(val)
    return None

# =================== нормальный парсер: группировка по проверкам ===================

def _parse_groups_normal(xml_path):
    """Возвращает dict: test_name -> [rows]; row = {'name','img','id','path','path_other'}."""
    root = _safe_xml_root(xml_path)
    base_dir = os.path.dirname(xml_path)

    # карта ребёнок->родитель для подъёма к <test>
    parent = {}
    for p in root.iter():
        for ch in list(p):
            parent[ch] = p

    def _test_name_for_cr(cr):
        # 1) приоритет — атрибут у clashresult
        tn = cr.get('testname') or cr.get('test') or cr.get('groupname')
        if tn:
            return tn
        # 2) поднимаемся по предкам
        p = parent.get(cr)
        while p is not None:
            tn = p.get('name') or p.get('displayname') or p.get('testname')
            if tn:
                return tn
            p = parent.get(p)
        return u'(Без названия проверки)'

    groups = {}
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
            rows.append({'name': clash_name, 'img': img_abs, 'id': a['id'], 'path': a['path'], 'path_other': b['path']})
            rows.append({'name': clash_name, 'img': img_abs, 'id': b['id'], 'path': b['path'], 'path_other': a['path']})
        else:
            ob = objs[0]
            rows.append({'name': clash_name, 'img': img_abs, 'id': ob['id'], 'path': ob['path'], 'path_other': u''})

    return groups

# =================== fallback (регулярки) ===================

_RX_TEST_BLOCK  = re.compile(r'<test\b[^>]*>.*?</test>', re.DOTALL | re.IGNORECASE)
_RX_TEST_NAME   = re.compile(r'\bname="([^"]+)"', re.IGNORECASE)
_RX_ATTR_TESTNM = re.compile(r'\btestname="([^"]+)"', re.IGNORECASE)
_RX_CR_BLOCK    = re.compile(r'<clashresult\b[^>]*>.*?</clashresult>', re.DOTALL | re.IGNORECASE)
_RX_ATTR_HREF   = re.compile(r'\bhref="([^"]*)"', re.IGNORECASE)
_RX_ATTR_CNAME  = re.compile(r'\bname="([^"]+)"', re.IGNORECASE)
_RX_CO_BLOCK    = re.compile(r'<clashobject\b[^>]*>.*?</clashobject>', re.DOTALL | re.IGNORECASE)
_RX_SMART_PAIR  = re.compile(r'<smarttag>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</smarttag>', re.DOTALL | re.IGNORECASE)
_RX_OA_PAIR     = re.compile(r'<objectattribute>.*?<name>(.*?)</name>.*?<value>(.*?)</value>.*?</objectattribute>', re.DOTALL | re.IGNORECASE)
_RX_PATHLINK    = re.compile(r'<pathlink>.*?</pathlink>', re.DOTALL | re.IGNORECASE)
_RX_NODE_TEXT   = re.compile(r'<node>(.*?)</node>', re.DOTALL | re.IGNORECASE)
_RX_TAGS        = re.compile(r'<[^>]+>')

def _parse_groups_fallback(xml_path):
    txt = _sanitize_xml_text(_read_text_guess_enc(xml_path))
    base_dir = os.path.dirname(xml_path)
    groups = {}

    test_blocks = list(_RX_TEST_BLOCK.finditer(txt))
    # если в файле нет <test> — разбираем весь документ, имя берём из clashresult@testname
    if not test_blocks:
        test_blocks = [type('M', (), {'group': (lambda self, n=0: txt)})()]

    for tb in test_blocks:
        tblock = tb.group(0) if hasattr(tb, 'group') else tb
        tname_m = _RX_TEST_NAME.search(tblock)
        default_tname = tname_m.group(1) if tname_m else u'(Без названия проверки)'

        for crm in _RX_CR_BLOCK.finditer(tblock):
            crblock = crm.group(0)
            # testname у clashresult — приоритет
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
                # pathlink
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
                rows.append({'name': cname, 'img': img_abs, 'id': a['id'], 'path': a['path'], 'path_other': b['path']})
                rows.append({'name': cname, 'img': img_abs, 'id': b['id'], 'path': b['path'], 'path_other': a['path']})
            else:
                ob = objs[0]
                rows.append({'name': cname, 'img': img_abs, 'id': ob['id'], 'path': ob['path'], 'path_other': u''})

    return groups

# =================== вывод ===================

def _only_existing(items):
    kept = []
    for it in items:
        try:
            if doc.GetElement(ElementId(it['id'])):
                kept.append(it)
        except Exception:
            pass
    return kept

def _img_cell(path, w=96):
    if not path or not os.path.exists(path):
        return u'—'
    uri = u'file:///' + path.replace('\\', '/')
    return u'<img src="{0}" width="{1}" />'.format(uri, int(w))

def _print_group(title, rows):
    rows = _only_existing(rows)
    if not rows:
        return
    out.print_md(u"\n---\n### Проверка: **{}**  _(строк: {})_".format(title, len(rows)))
    table_data = []
    for i, it in enumerate(rows, 1):
        table_data.append([
            i,
            _img_cell(it['img'], w=96),
            it['name'],
            out.linkify(ElementId(it['id'])),
            it['path'] or u'—',
            it['path_other'] or u'—'
        ])
    out.print_table(
        table_data=table_data,
        columns=[u'№', u'Снимок', u'Пересечение', u'ID', u'Путь элемента', u'Путь второго элемента'],
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

    total = sum(len(v) for v in groups.values())
    if total == 0:
        forms.alert(
            u"Не удалось извлечь элементы из отчёта. Проверьте, что включены «Путь к элементу» и «Идентификатор элемента».",
            title=u"Пересечения"
        )
        return

    out.print_md(u"## Пересечения: {} строк ({} проверок)".format(total, len(groups)))
    # по алфавиту; если нужен «порядок как в файле», можно не сортировать
    for test_name in sorted(groups.keys(), key=lambda s: s.lower()):
        _print_group(test_name, groups[test_name])

    out.print_md(u"_Клик по **ID** выделяет элемент. Можно кликать подряд._")

if __name__ == "__main__":
    main()
