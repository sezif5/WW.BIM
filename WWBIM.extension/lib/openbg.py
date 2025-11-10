# -*- coding: utf-8 -*-
"""
openbg.py — фон. открытие RVT + безопасная подготовка 3D‑вида для NWC.
Важное изменение: полностью исключён прямой доступ к BuiltInCategory через getattr().
Теперь используется только Enum.IsDefined/Enum.Parse — это устраняет AttributeError
в сборках, где, например, отсутствует OST_ImportsInFamilies (Revit 2022).
"""

from Autodesk.Revit.DB import (
    ModelPathUtils, WorksharingUtils, WorksetId,
    OpenOptions, WorksetConfiguration, WorksetConfigurationOption,
    BuiltInCategory, Category, ElementId, View3D, ViewFamilyType, ViewFamily, Transaction, FilteredElementCollector
)
from System.Collections.Generic import List
from System import Enum

# ---------------- helpers ----------------

def _enum(name):
    try:
        return Enum.Parse(WorksetConfigurationOption, name)
    except Exception:
        return Enum.Parse(WorksetConfigurationOption, 'OpenAllWorksets')

def _cfg_from_optname(name):
    opt = _enum(name)
    return WorksetConfiguration(opt)

def _is_string(x):
    try:
        return isinstance(x, basestring)
    except NameError:
        try:
            return isinstance(x, str) or isinstance(x, unicode)
        except NameError:
            return isinstance(x, str)

def _coerce_app_uiapp(app_or_uiapp, maybe_uiapp=None):
    if maybe_uiapp is not None:
        return app_or_uiapp, maybe_uiapp
    try:
        _ = app_or_uiapp.Application
        uiapp = app_or_uiapp
        app = uiapp.Application
    except Exception:
        app = app_or_uiapp
        uiapp = None
    return app, uiapp

def _to_model_path(path_or_mp):
    try:
        if hasattr(path_or_mp, "ServerPath") or hasattr(path_or_mp, "CentralServerPath"):
            return path_or_mp
    except Exception:
        pass
    return ModelPathUtils.ConvertUserVisiblePathToModelPath(path_or_mp)

def _get_workset_previews(uiapp, mp):
    previews = None
    if uiapp is not None:
        try:
            previews = WorksharingUtils.GetUserWorksetInfoForOpen(uiapp, mp)
        except Exception:
            previews = None
    if previews is None:
        try:
            previews = WorksharingUtils.GetUserWorksetInfo(mp)
        except Exception:
            previews = []
    return list(previews or [])

def _ids_all_except_prefixes(previews, prefixes):
    ids = List[WorksetId]()
    for p in previews:
        try:
            nm = p.Name or u""
            if not any(nm.startswith(pr) for pr in prefixes):
                ids.Add(p.Id)
        except Exception:
            pass
    return ids

def _ids_only_prefixes(previews, prefixes):
    ids = List[WorksetId]()
    for p in previews:
        try:
            nm = p.Name or u""
            if any(nm.startswith(pr) for pr in prefixes):
                ids.Add(p.Id)
        except Exception:
            pass
    return ids

def _ids_only_names(previews, names, case_sensitive=False):
    ids = List[WorksetId]()
    if not names:
        return ids
    if case_sensitive:
        names_set = set(names)
        for p in previews:
            try:
                if (p.Name or u"") in names_set:
                    ids.Add(p.Id)
            except Exception:
                pass
    else:
        names_l = set([(n or u"").lower() for n in names])
        for p in previews:
            try:
                if (p.Name or u"").lower() in names_l:
                    ids.Add(p.Id)
            except Exception:
                pass
    return ids

def _ids_by_predicate(previews, pred):
    ids = List[WorksetId]()
    if not callable(pred):
        return ids
    for p in previews:
        try:
            if pred(p.Name or u""):
                ids.Add(p.Id)
        except Exception:
            pass
    return ids

def _build_ws_config(uiapp, mp, worksets_rule):
    if _is_string(worksets_rule):
        key = (worksets_rule or '').strip().lower()
        if key in ('all', 'open_all'):
            return _cfg_from_optname('OpenAllWorksets')
        if key in ('close', 'close_all'):
            return _cfg_from_optname('CloseAllWorksets')
        if key in ('lastviewed', 'last_viewed', 'last'):
            return _cfg_from_optname('LastViewed')

    previews = _get_workset_previews(uiapp, mp)
    if not previews:
        return _cfg_from_optname('LastViewed')

    cfg = _cfg_from_optname('CloseAllWorksets')

    if _is_string(worksets_rule) and (worksets_rule or '').strip().lower() == 'all_except_00':
        ids = _ids_all_except_prefixes(previews, (u'00_',))
        if ids.Count > 0: cfg.Open(ids)
        return cfg

    if isinstance(worksets_rule, tuple) and len(worksets_rule) > 0:
        mode = ((worksets_rule[0] or '') if len(worksets_rule) > 0 else '').strip().lower()
        if mode == 'all_except_prefixes':
            prefixes = tuple(worksets_rule[1]) if len(worksets_rule) > 1 else (u'00_',)
            ids = _ids_all_except_prefixes(previews, prefixes); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg
        if mode == 'only_prefixes':
            prefixes = tuple(worksets_rule[1]) if len(worksets_rule) > 1 else tuple()
            ids = _ids_only_prefixes(previews, prefixes); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg
        if mode == 'only_names':
            names = tuple(worksets_rule[1]) if len(worksets_rule) > 1 else tuple()
            ids = _ids_only_names(previews, names, case_sensitive=False); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg
        if mode == 'predicate':
            pred = worksets_rule[1] if len(worksets_rule) > 1 else None
            ids = _ids_by_predicate(previews, pred); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg

    if isinstance(worksets_rule, dict):
        mode = (worksets_rule.get('mode') or '').strip().lower()
        if mode == 'all_except_prefixes':
            prefixes = tuple(worksets_rule.get('prefixes') or (u'00_',))
            ids = _ids_all_except_prefixes(previews, prefixes); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg
        if mode == 'only_prefixes':
            prefixes = tuple(worksets_rule.get('prefixes') or tuple())
            ids = _ids_only_prefixes(previews, prefixes); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg
        if mode == 'only_names':
            names = tuple(worksets_rule.get('names') or tuple())
            case = bool(worksets_rule.get('case_sensitive', False))
            ids = _ids_only_names(previews, names, case_sensitive=case); 
            if ids.Count > 0: cfg.Open(ids)
            return cfg

    return _cfg_from_optname('LastViewed')

# ----------- BIC safe -----------

def _resolve_bic(name):
    """Вернуть BuiltInCategory по имени или None, если такой константы нет в этой версии."""
    if not name: 
        return None
    try:
        if Enum.IsDefined(BuiltInCategory, name):
            return Enum.Parse(BuiltInCategory, name)
    except Exception:
        pass
    return None

def _cat_id(doc, bic):
    if bic is None: 
        return None
    try:
        cat = Category.GetCategory(doc, bic)
        if cat: return cat.Id
    except Exception:
        return None
    return None

def _hide_categories_by_names(doc, view, names):
    ids = List[ElementId]()
    for nm in (names or []):
        bic = _resolve_bic(nm)
        eid = _cat_id(doc, bic)
        if eid: ids.Add(eid)

    if ids.Count == 0: 
        return 0

    hidden = 0
    try:
        view.HideCategories(ids)
        return ids.Count
    except Exception:
        pass

    for eid in ids:
        try:
            view.SetCategoryHidden(eid, True)
            hidden += 1
        except Exception:
            try:
                view.SetCategoryHidden(eid.IntegerValue, True)
                hidden += 1
            except Exception:
                pass
    return hidden

# ----------- public API -----------

def open_in_background(app_or_uiapp, maybe_uiapp, model_path_or_str, audit=False, worksets='lastviewed'):
    app, uiapp = _coerce_app_uiapp(app_or_uiapp, maybe_uiapp)
    mp = _to_model_path(model_path_or_str)

    cfg = _build_ws_config(uiapp, mp, worksets)

    opts = OpenOptions()
    try: opts.Audit = bool(audit)
    except Exception: pass
    try: opts.SetOpenWorksetsConfiguration(cfg)
    except Exception: pass

    try:
        return app.OpenDocumentFile(mp, opts)
    except Exception as ex1:
        msg = u"{}".format(ex1)
        need_retry_lv = False
        key = (worksets or '').strip().lower() if _is_string(worksets) else u''
        if key in (u'lastviewed', u'last_viewed', u'last'): need_retry_lv = True
        if 'LastViewed' in msg or ('attribute' in msg and 'LastViewed' in msg): need_retry_lv = True

        if need_retry_lv:
            try:
                cfg2 = _build_ws_config(uiapp, mp, 'all_except_00')
                opts2 = OpenOptions(); 
                try: opts2.Audit = bool(audit)
                except Exception: pass
                try: opts2.SetOpenWorksetsConfiguration(cfg2)
                except Exception: pass
                return app.OpenDocumentFile(mp, opts2)
            except Exception:
                cfg3 = _cfg_from_optname('OpenAllWorksets')
                opts3 = OpenOptions(); 
                try: opts3.Audit = bool(audit)
                except Exception: pass
                try: opts3.SetOpenWorksetsConfiguration(cfg3)
                except Exception: pass
                return app.OpenDocumentFile(mp, opts3)
        raise

def prepare_navisworks_view(doc, view):
    """Скрыть служебные/аннотационные категории. Отсутствующие — пропускаются. Возвращает число скрытых."""
    names = [
        # ссылки и импорты
        'OST_RvtLinks', 'OST_LinkInstances', 'OST_ExportLayer', 'OST_ImportInstance', 'OST_ImportsInFamilies',
        # служебное
        'OST_Cameras', 'OST_Views', 'OST_Lines', 'OST_PointClouds', 'OST_PointCloudsHardware', 'OST_Levels', 'OST_Grids',
        # аннотации
        'OST_Annotations', 'OST_TitleBlocks', 'OST_Viewports', 'OST_TextNotes', 'OST_Dimensions'
    ]
    return _hide_categories_by_names(doc, view, names)

def get_or_create_navisworks_view(doc, name=u'Navisworks'):
    """Найдёт или создаст изометрический 3D‑вид с именем name. Удобно для батч‑экспорта."""
    # поиск
    try:
        for v in FilteredElementCollector(doc).OfClass(View3D):
            try:
                if not v.IsTemplate and v.Name == name:
                    return v
            except Exception:
                pass
    except Exception:
        pass

    # создание
    vft = None
    try:
        for t in FilteredElementCollector(doc).OfClass(ViewFamilyType):
            if t.ViewFamily == ViewFamily.ThreeDimensional:
                vft = t; break
    except Exception:
        pass
    if vft is None:
        return None

    created = None
    t = Transaction(doc, u'Create 3D View for Navisworks')
    t.Start()
    try:
        created = View3D.CreateIsometric(doc, vft.Id)
        try: created.Name = name
        except Exception: pass
        t.Commit()
    except Exception:
        try: t.RollBack()
        except Exception: pass
        created = None
    return created
