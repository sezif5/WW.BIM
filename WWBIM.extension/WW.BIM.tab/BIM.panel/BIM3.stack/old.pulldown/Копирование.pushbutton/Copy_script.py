# -*- coding: utf-8 -*-
# pyRevit button: Copy values between parameters on active view (model-only)
# Author: ChatGPT (GPT-5 Thinking)

from __future__ import print_function, division
import sys
import traceback
from pyrevit import revit, DB, forms

doc = revit.doc
uidoc = revit.uidoc
active_view = doc.ActiveView

# --------------------------- Helpers ---------------------------

class ParamItem(object):
    def __init__(self, name, is_type):
        self.name = name
        self.is_type = bool(is_type)
        self.display = u"{0} ({1})".format(name, u"Тип" if is_type else u"Экземпляр")
    def __unicode__(self):
        return self.display
    def __str__(self):
        try:
            return self.display.encode('utf-8')
        except:
            return self.display

def _is_real_model_elem(el):
    """Оставляем только реальные модельные элементы:
       - CategoryType == Model
       - НЕ view-specific (OwnerViewId == InvalidElementId)
       - Не типы (собираем их отдельно через WhereElementIsNotElementType)
    """
    if el is None:
        return False
    # Отсечь виды на всякий случай
    if isinstance(el, DB.View):
        return False

    cat = el.Category
    if cat is None:
        return False

    # Только модельные категории
    try:
        if cat.CategoryType != DB.CategoryType.Model:
            return False
    except:
        return False

    # Исключаем всё, что привязано к конкретному виду (аннотации, камеры и т.п.)
    try:
        if el.OwnerViewId != DB.ElementId.InvalidElementId:
            return False
    except:
        # Если свойства нет — ок, оставляем
        pass

    return True

def _iter_visible_instances_on_active_view(document, view):
    """Экземпляры элементов, реально присутствующие на активном виде:
       - Collector по view.Id
       - Не типы
       - Фильтр _is_real_model_elem
    """
    fec = (DB.FilteredElementCollector(document, view.Id)
           .WhereElementIsNotElementType())
    for el in fec:
        try:
            if not _is_real_model_elem(el):
                continue
        except:
            continue
        yield el

def _collect_param_items(elements):
    seen_inst = set()
    seen_type = set()
    inst_items, type_items = [], []

    elements = list(elements)  # реальный проход дважды

    for el in elements:
        # экземпляры
        try:
            for p in el.Parameters:
                try:
                    n = p.Definition.Name
                except:
                    continue
                if (n,) not in seen_inst:
                    seen_inst.add((n,))
                    inst_items.append(ParamItem(n, is_type=False))
        except:
            pass

        # типы
        try:
            tid = el.GetTypeId()
            if tid and tid != DB.ElementId.InvalidElementId:
                typ = doc.GetElement(tid)
                if typ:
                    for p in typ.Parameters:
                        try:
                            n = p.Definition.Name
                        except:
                            continue
                        if (n,) not in seen_type:
                            seen_type.add((n,))
                            type_items.append(ParamItem(n, is_type=True))
        except:
            pass

    merged = inst_items + type_items
    merged.sort(key=lambda x: x.display.lower())
    return merged, merged[:]

def _lookup_param(elem, pitem):
    if pitem.is_type:
        tid = elem.GetTypeId()
        if not tid or tid == DB.ElementId.InvalidElementId:
            return None
        typ = doc.GetElement(tid)
        if typ is None:
            return None
        return typ.LookupParameter(pitem.name)
    else:
        return elem.LookupParameter(pitem.name)

def _param_storage_type(param):
    try:
        return param.StorageType
    except:
        return None

def _get_as_string(param):
    if param is None:
        return None
    try:
        vs = param.AsValueString()
        if vs not in (None, u"", ""):
            return vs
    except:
        pass

    st = _param_storage_type(param)
    try:
        if st == DB.StorageType.String:
            return param.AsString()
        elif st == DB.StorageType.Double:
            return unicode(param.AsDouble())
        elif st == DB.StorageType.Integer:
            return unicode(param.AsInteger())
        elif st == DB.StorageType.ElementId:
            eid = param.AsElementId()
            if isinstance(eid, DB.ElementId):
                return unicode(eid.IntegerValue)
            return None
    except:
        return None
    return None

def _get_raw_value(param):
    st = _param_storage_type(param)
    if st == DB.StorageType.String:
        return param.AsString()
    elif st == DB.StorageType.Double:
        return param.AsDouble()
    elif st == DB.StorageType.Integer:
        return param.AsInteger()
    elif st == DB.StorageType.ElementId:
        return param.AsElementId()
    return None

def _try_set_from_string(target_param, text):
    if target_param.IsReadOnly:
        return False, u"readonly"

    try:
        target_param.SetValueString(text)
        return True, None
    except:
        pass

    st = _param_storage_type(target_param)
    t = (text or u"").strip()
    if t == u"":
        return False, u"empty->numeric"

    if st == DB.StorageType.Integer:
        lower = t.lower()
        if lower in (u"да", u"yes", u"true", u"истина", u"on", u"1"):
            try:
                target_param.Set(int(1)); return True, None
            except:
                return False, u"int-set-failed"
        if lower in (u"нет", u"no", u"false", u"ложь", u"off", u"0"):
            try:
                target_param.Set(int(0)); return True, None
            except:
                return False, u"int-set-failed"
        try:
            target_param.Set(int(int(t))); return True, None
        except:
            return False, u"int-parse"

    if st == DB.StorageType.Double:
        tt = t.replace(u",", u".")
        try:
            target_param.Set(float(float(tt))); return True, None
        except:
            return False, u"double-parse"

    if st == DB.StorageType.ElementId:
        try:
            target_param.Set(DB.ElementId(int(t))); return True, None
        except:
            return False, u"eid-parse"

    return False, u"unsupported-set-from-string"

def _copy_value(src_param, dst_param):
    if src_param is None or dst_param is None:
        return False, u"param-missing"
    if dst_param.IsReadOnly:
        return False, u"readonly"

    sst = _param_storage_type(src_param)
    dst = _param_storage_type(dst_param)

    if sst == dst:
        try:
            raw = _get_raw_value(src_param)
            if dst == DB.StorageType.ElementId and not isinstance(raw, DB.ElementId):
                return False, u"eid-bad"
            dst_param.Set(raw)
            return True, None
        except:
            return False, u"set-same-failed"

    if dst == DB.StorageType.String:
        sval = _get_as_string(src_param)
        try:
            dst_param.Set(sval); return True, None
        except:
            return False, u"set-string-failed"

    if sst == DB.StorageType.String and dst in (DB.StorageType.Double, DB.StorageType.Integer, DB.StorageType.ElementId):
        return _try_set_from_string(dst_param, src_param.AsString())

    if dst == DB.StorageType.Integer and sst == DB.StorageType.Double:
        try:
            dst_param.Set(int(round(src_param.AsDouble()))); return True, None
        except:
            return False, u"double->int-failed"

    if dst == DB.StorageType.Double and sst == DB.StorageType.Integer:
        try:
            dst_param.Set(float(src_param.AsInteger())); return True, None
        except:
            return False, u"int->double-failed"

    return False, u"incompatible"

# --------------------------- UI ---------------------------

elements_on_view = list(_iter_visible_instances_on_active_view(doc, active_view))
if not elements_on_view:
    forms.alert(u"На активном виде нет подходящих модельных элементов.", title=u"Копирование параметров", warn_icon=True)
    sys.exit()

source_items, target_items = _collect_param_items(elements_on_view)
if not source_items:
    forms.alert(u"Не удалось собрать список параметров. Проверьте элементы на виде.", title=u"Копирование параметров", warn_icon=True)
    sys.exit()

src_pick = forms.SelectFromList.show(
    source_items, title=u"Из какого параметра копировать?",
    multiselect=False, name_attr='display', width=600, height=600, button_name=u"Далее"
)
if not src_pick: sys.exit()
src_item = src_pick if isinstance(src_pick, ParamItem) else src_pick[0]

dst_pick = forms.SelectFromList.show(
    target_items, title=u"В какой параметр копировать?",
    multiselect=False, name_attr='display', width=600, height=600, button_name=u"Копировать"
)
if not dst_pick: sys.exit()
dst_item = dst_pick if isinstance(dst_pick, ParamItem) else dst_pick[0]

if dst_item.is_type:
    forms.alert(
        u"Целевой параметр — ПАРАМЕТР ТИПА.\n\n"
        u"Значение будет задано на тип и затронет все экземпляры этого типа.\n"
        u"При копировании из экземпляра берётся значение первого встреченного экземпляра каждого типа.",
        title=u"Предупреждение", warn_icon=True
    )

# --------------------------- Copy pass ---------------------------

total = len(elements_on_view)
done = miss_src = miss_dst = readonly = incompat = errors = 0
typedone = set()

t = DB.Transaction(doc, u"Копирование параметров: {0} → {1}".format(src_item.display, dst_item.display))
t.Start()
try:
    with forms.ProgressBar(title=u"Копирование параметров…", cancellable=True, step=1) as pb:
        for idx, el in enumerate(elements_on_view):
            if pb.cancelled: break
            try:
                psrc = _lookup_param(el, src_item)
                pdst = _lookup_param(el, dst_item)

                if psrc is None:
                    miss_src += 1; pb.update_progress(idx+1, total); continue
                if pdst is None:
                    miss_dst += 1; pb.update_progress(idx+1, total); continue

                if dst_item.is_type:
                    tid = el.GetTypeId()
                    if tid and tid != DB.ElementId.InvalidElementId:
                        if tid in typedone:
                            pb.update_progress(idx+1, total); continue

                ok, reason = _copy_value(psrc, pdst)
                if ok:
                    done += 1
                    if dst_item.is_type:
                        tid = el.GetTypeId()
                        if tid and tid != DB.ElementId.InvalidElementId:
                            typedone.add(tid)
                else:
                    if reason == u"readonly":
                        readonly += 1
                    elif reason in (u"incompatible", u"double->int-failed", u"int->double-failed",
                                    u"set-string-failed", u"set-same-failed",
                                    u"empty->numeric", u"int-parse", u"double-parse",
                                    u"eid-parse", u"unsupported-set-from-string", u"eid-bad"):
                        incompat += 1
                    else:
                        errors += 1
            except:
                errors += 1
            pb.update_progress(idx+1, total)
    t.Commit()
except Exception as ex:
    t.RollBack()
    traceback.print_exc()
    forms.alert(u"Ошибка при копировании: {0}".format(ex), title=u"Копирование параметров", warn_icon=True)
    sys.exit()

# --------------------------- Report ---------------------------
msg = u"\n".join([
    u"Подходящих элементов на виде: {0}".format(total),
    u"Скопировано значений: {0}".format(done),
    u"Источник не найден: {0}".format(miss_src),
    u"Приёмник не найден: {0}".format(miss_dst),
    u"Только для чтения: {0}".format(readonly),
    u"Несовместимо/ошибка конвертации: {0}".format(incompat),
    u"Прочие ошибки: {0}".format(errors),
])
forms.alert(msg, title=u"Готово", warn_icon=False)
