# -*- coding: utf-8 -*-
# Links → Worksets v2 — fix: set ELEM_PARTITION_PARAM with ElementId + optional pinning after assignment

import os
import re
import traceback
import clr
clr.AddReference('RevitAPI')
clr.AddReference('RevitServices')

from Autodesk.Revit.DB import (
    FilteredElementCollector, RevitLinkInstance, RevitLinkType, ElementId,
    BuiltInParameter, Transaction, Workset, WorksetKind, FilteredWorksetCollector,
    WorksetDefaultVisibilitySettings, WorksetVisibility, View, ModelPathUtils,
    ImportInstance, StorageType
)
from RevitServices.Persistence import DocumentManager

# --- pyRevit helpers ---
try:
    from pyrevit import script, forms
    logger  = script.get_logger()
    output  = script.get_output()
except Exception:
    class _Dummy(object):
        def info(self, *a, **k):    print(" ".join(map(str, a)))
        def warning(self, *a, **k): print(" ".join(map(str, a)))
        def error(self, *a, **k):   print(" ".join(map(str, a)))
        def print_md(self, *a, **k): print(" ".join(map(str, a)))
    logger, forms, output = _Dummy(), None, _Dummy()

def alert(msg, title='Links → Worksets v2'):
    logger.error(msg)
    try:
        if forms: forms.alert(msg, title=title)
    except Exception:
        pass

# --- settings ---
ALWAYS_PIN_AFTER_ASSIGN = True  # закреплять (Pin) связи после распределения по РН

# --- get doc safely ---
def get_current_doc():
    try:
        uidoc = __revit__.ActiveUIDocument  # noqa
        if uidoc: return uidoc.Document
    except Exception:
        pass
    try:
        doc = DocumentManager.Instance.CurrentDBDocument
        if doc: return doc
    except Exception:
        pass
    return None

# --- mapping ---
RULES = [
    ({u"КООРД", u"KOORD"},            u"00_Связи_RVT_00_КООРД", True),
    ({u"АР", u"AR"},                  u"00_Связи_RVT_01_АР",    False),
    ({u"КР", u"KR"},                  u"00_Связи_RVT_02_КР",    False),
    ({u"ОВ", u"OV"},                  u"00_Связи_RVT_03_ОВ",    False),
    ({u"ВК", u"VK"},                  u"00_Связи_RVT_04_ВК",    False),
    ({u"ВНС", u"VNS"},                  u"00_Связи_RVT_04_ВНС",    False),
    ({u"ЭОМ", u"EOM"},                u"00_Связи_RVT_05_ЭОМ",   False),
    ({u"СС", u"SS"},                  u"00_Связи_RVT_06_СС",    False),
    ({u"ИТП", u"ITP"},                u"00_Связи_RVT_03_ИТП",   False),
    ({u"ПТ",  u"PT"},                 u"00_Связи_RVT_04_ПТ",    False),
]

LAT2CYR = {
    u"A": u"А", u"B": u"В", u"C": u"С", u"E": u"Е", u"H": u"Н",
    u"K": u"К", u"M": u"М", u"O": u"О", u"P": u"Р", u"T": u"Т",
    u"X": u"Х", u"Y": u"У"
}
SEG_SPLIT = re.compile(r"[\W_]+", re.UNICODE)

def normalize_cyr(s):
    if not s: return u""
    up = s.upper()
    return u"".join(LAT2CYR.get(ch, ch) for ch in up)

def split_segments(text):
    norm = normalize_cyr(text or u"")
    return [seg for seg in SEG_SPLIT.split(norm) if seg]

def segments_from_link(doc, ltype, linstr):
    segs = []
    try: segs += split_segments(getattr(ltype, "Name", u""))
    except Exception: pass
    try: segs += split_segments(getattr(linstr, "Name", u""))
    except Exception: pass
    try:
        efr = ltype.GetExternalFileReference()
        if efr:
            upath = ModelPathUtils.ConvertModelPathToUserVisiblePath(efr.GetAbsolutePath())
            if upath:
                base = os.path.splitext(os.path.basename(upath))[0]
                segs += split_segments(base)
    except Exception:
        pass
    seen, uniq = set(), []
    for s in segs:
        if s not in seen:
            uniq.append(s); seen.add(s)
    return uniq

def match_rule(segments):
    for tokens, wsname, hide in RULES:
        for tok in tokens:
            for seg in segments:
                if seg == tok or seg.startswith(tok):
                    return wsname, hide, tok
    return None, None, None

# --- workset utils ---
def ensure_workset(doc, ws_name):
    existing = [ws for ws in FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset)
                if ws.Name == ws_name]
    if existing:
        return existing[0], False
    t = Transaction(doc, u"Создать рабочий набор: {0}".format(ws_name))
    t.Start(); ws = Workset.Create(doc, ws_name); t.Commit()
    return ws, True

def set_default_visibility(doc, workset, visible):
    settings = WorksetDefaultVisibilitySettings.GetWorksetDefaultVisibilitySettings(doc)
    t = Transaction(doc, u"Дефолтная видимость РН: {0}".format(workset.Name))
    t.Start(); settings.SetVisibility(workset.Id, bool(visible)); t.Commit()

def hide_workset_in_all_views(doc, workset):
    views = FilteredElementCollector(doc).OfClass(View)
    cnt = 0
    t = Transaction(doc, u"Скрыть РН во всех видах: {0}".format(workset.Name))
    t.Start()
    for v in views:
        try:
            if v and not v.IsTemplate:
                v.SetWorksetVisibility(workset.Id, WorksetVisibility.Hidden)
                cnt += 1
        except Exception:
            pass
    t.Commit()
    return cnt

def assign_to_workset(elem, workset, stats):
    p = elem.get_Parameter(BuiltInParameter.ELEM_PARTITION_PARAM)
    if p is None:
        stats["no_param"] += 1
        return False, u"нет параметра ELEM_PARTITION_PARAM"
    if p.IsReadOnly:
        stats["readonly"] += 1
        return False, u"параметр только для чтения"
    try:
        # Правильный способ: параметр типа ElementId → задаём ElementId рабочего набора
        if p.StorageType == StorageType.ElementId:
            ok = p.Set(workset.Id)
        else:
            ok = p.Set(workset.Id.IntegerValue)
        if not ok:
            stats["failed_set"] += 1
            return False, u"Set(...) вернул False"
        # Пинним связь по требованию
        if ALWAYS_PIN_AFTER_ASSIGN and (isinstance(elem, RevitLinkInstance) or isinstance(elem, ImportInstance)):
            try:
                elem.Pinned = True
            except Exception:
                pass
        return True, u"ok"
    except Exception as ex:
        stats["exceptions"] += 1
        return False, u"исключение: {0}".format(ex)

def main():
    doc = get_current_doc()
    if doc is None:
        alert(u"Не удалось получить активный документ Revit. Откройте проект и запустите ещё раз.")
        return
    if doc.IsFamilyDocument:
        alert(u"Скрипт работает только в проектных файлах, а не в семействах.")
        return
    if not doc.IsWorkshared:
        alert(u"В модели не включено совместное использование.\nВключите «Рабочие наборы» и запустите скрипт снова.")
        return

    types = list(FilteredElementCollector(doc).OfClass(RevitLinkType))
    insts = list(FilteredElementCollector(doc).OfClass(RevitLinkInstance))
    if not types or not insts:
        alert(u"В файле нет Revit-связей (типов или экземпляров).")
        return

    insts_by_type = {}
    for inst in insts:
        try:
            insts_by_type.setdefault(inst.GetTypeId(), []).append(inst)
        except Exception:
            pass

    plan = []
    required_ws = set()
    ws_to_hide = set()
    rows = []

    for ltype in types:
        tname = getattr(ltype, "Name", u"") or u""
        for inst in insts_by_type.get(ltype.Id, []):
            segs = segments_from_link(doc, ltype, inst)
            wsname, hide, tok = match_rule(segs)
            rows.append([tname, getattr(inst, "Name", u"") or u"", u" ".join(segs), tok or u"—"])
            if wsname:
                plan.append((inst, wsname, hide))
                required_ws.add(wsname)
                if hide:
                    ws_to_hide.add(wsname)

    DWG_WS_NAME = u"00_Связи_DWG"
    cad_insts = list(FilteredElementCollector(doc).OfClass(ImportInstance))
    if cad_insts:
        for cad in cad_insts:
            plan.append((cad, DWG_WS_NAME, False))
        required_ws.add(DWG_WS_NAME)

    if not plan:
        output.print_md("### Диагностика: тип | экземпляр | сегменты | токен")
        output.print_md("| Тип | Экземпляр | Сегменты | Токен |")
        output.print_md("|---|---|---|---|")
        for r in rows:
            output.print_md(u"| {0} | {1} | {2} | {3} |".format(*r))
        alert(u"Подходящих RVT-связей по заданным токенам не найдено. Смотри панель pyRevit.")
        return

    ws_cache, created_ws = {}, []
    for name in sorted(required_ws):
        ws, created = ensure_workset(doc, name)
        ws_cache[name] = ws
        if created: created_ws.append(name)

    stats = {"no_param":0, "readonly":0, "failed_set":0, "exceptions":0}
    moved = 0
    details = []
    t = Transaction(doc, u"Распределить связи по рабочим наборам")
    t.Start()
    for inst, wsname, _hide in plan:
        ok, msg = assign_to_workset(inst, ws_cache[wsname], stats)
        if ok:
            moved += 1
        details.append([getattr(inst, "Name", u""), wsname, u"OK" if ok else (u"FAIL: " + msg)])
    t.Commit()

    hidden_report = []
    for name in sorted(ws_to_hide):
        ws = ws_cache.get(name)
        if not ws:
            continue
        try:
            set_default_visibility(doc, ws, False)
        except Exception as ex:
            logger.warning(u"Не удалось задать дефолтную видимость {0}: {1}".format(name, ex))
        try:
            hidden_cnt = hide_workset_in_all_views(doc, ws)
        except Exception as ex:
            logger.warning(u"Не удалось скрыть РН {0} во всех видах: {1}".format(name, ex))
            hidden_cnt = 0
        hidden_report.append((name, hidden_cnt))

    # отчёт
    output.print_md("### Выполнено")
    output.print_md("Перемещено/закреплено: **{0}**".format(moved))
    if created_ws:
        output.print_md("Созданы РН: " + ", ".join(created_ws))
    if any(stats.values()):
        output.print_md("Пропуски: no_param={no_param}, readonly={readonly}, failed_set={failed_set}, exceptions={exceptions}".format(**stats))
    output.print_md("### Подробности по экземплярам")
    output.print_md("| Экземпляр | РН | Результат |")
    output.print_md("|---|---|---|")
    for n, ws, st in details:
        output.print_md(u"| {0} | {1} | {2} |".format(n, ws, st))
    if hidden_report:
        for name, cnt in hidden_report:
            output.print_md(u"РН «{0}»: дефолтная видимость выключена; скрыто в видах: {1}".format(name, cnt))

    try:
        if forms:
            forms.alert(u"Готово ✅\nПеремещено/закреплено: {0}\nСозданы РН: {1}".format(
                moved, (u", ".join(created_ws) if created_ws else u"—")
            ), title='Links → Worksets v2')
    except Exception:
        pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        alert(u"Ошибка: {0}\n{1}".format(e, traceback.format_exc()))