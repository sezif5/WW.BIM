# -*- coding: utf-8 -*-
# pyRevit button script: выбрать элемент в связи и скопировать его ID
# IronPython / RevitAPI / pyRevit

from Autodesk.Revit.DB import ElementId, RevitLinkInstance
from Autodesk.Revit.UI import TaskDialog
from Autodesk.Revit.UI.Selection import ObjectType
from Autodesk.Revit.Exceptions import OperationCanceledException
from System.Collections.Generic import List
from System.Threading import Thread, ThreadStart, ApartmentState, Thread as _Thread

uidoc = __revit__.ActiveUIDocument
doc = uidoc.Document


def _show(title, text):
    try:
        TaskDialog.Show(title, text)
    except:
        pass


def _select_only(eid):
    try:
        ids = List[ElementId]()
        ids.Add(eid)
        uidoc.Selection.SetElementIds(ids)
    except:
        pass


def _zoom_to(eid):
    try:
        uidoc.ShowElements(eid)
    except:
        pass


def _try_in_sta(target_func):
    """Запустить target_func() в отдельном STA-потоке и дождаться завершения.
    target_func должен выставить result[0] = True/False и вернуть через замыкание."""
    result = [False]

    def runner():
        try:
            result[0] = bool(target_func())
        except:
            result[0] = False

    t = Thread(ThreadStart(runner))
    t.SetApartmentState(ApartmentState.STA)
    t.Start()
    t.Join()
    return result[0]


def _copy_to_clipboard(text):
    """Устойчивое копирование в буфер:
       1) pyRevit forms.toClipboard
       2) WinForms Clipboard
       3) WPF Clipboard
       Все — в STA и с ретраями (клипборд может быть занят другим процессом)."""
    if text is None:
        return False
    s = str(text)

    # 1) pyRevit helper (обычно самый надежный)
    try:
        from pyrevit import forms
        forms.toClipboard(s)
        return True
    except:
        pass

    # 2) WinForms Clipboard в STA с ретраями
    def _winforms_clip():
        try:
            from System.Windows.Forms import Clipboard as WFClipboard
        except:
            return False
        for _ in range(6):
            try:
                WFClipboard.SetText(s)
                return True
            except:
                _Thread.Sleep(60)
        return False

    if _try_in_sta(_winforms_clip):
        return True

    # 3) WPF Clipboard в STA с ретраями
    def _wpf_clip():
        try:
            from System.Windows import Clipboard as WPFClipboard
        except:
            return False
        for _ in range(6):
            try:
                WPFClipboard.SetText(s)
                return True
            except:
                _Thread.Sleep(60)
        return False

    if _try_in_sta(_wpf_clip):
        return True

    return False


def main():
    ref = None
    # 1) Пробуем выбрать именно связанный элемент (новые версии Revit)
    try:
        ref = uidoc.Selection.PickObject(ObjectType.LinkedElement, u"Выберите элемент в связанном файле")
    except:
        # 2) Фолбэк — любой элемент (у ссылочного будет LinkedElementId)
        try:
            ref = uidoc.Selection.PickObject(ObjectType.Element, u"Выберите элемент (поддерживаются связанные)")
        except OperationCanceledException:
            return
        except:
            _show(u"Выбор элемента", u"Не удалось выбрать элемент.")
            return

    if ref is None:
        return

    # Если кликнули по элементу в связи — у Reference есть LinkedElementId
    linked_id = None
    try:
        linked_id = getattr(ref, "LinkedElementId", None)
    except:
        linked_id = None

    # A) Связанный элемент
    if linked_id and linked_id != ElementId.InvalidElementId:
        link_inst = doc.GetElement(ref.ElementId)
        if isinstance(link_inst, RevitLinkInstance):
            try:
                linked_doc = link_inst.GetLinkDocument()
            except:
                linked_doc = None

            if linked_doc is None:
                _show(u"Связанный элемент", u"Документ связи недоступен (возможно, выгружен).")
                return

            linked_el = linked_doc.GetElement(linked_id)
            if linked_el is None:
                _show(u"Связанный элемент", u"Не удалось получить элемент в связи.")
                return

            id_text = str(linked_id.IntegerValue)

            # Выделяем и показываем экземпляр связи (сам элемент в связи Revit выделить не может)
            _select_only(link_inst.Id)
            _zoom_to(link_inst.Id)

            copied = _copy_to_clipboard(id_text)
            msg = u"ID связанного элемента: {0}".format(id_text)
            msg += u"\nID скопирован в буфер обмена." if copied else u"\nНе удалось скопировать ID в буфер обмена."
            _show(u"Выбор элемента (связь)", msg)
            return

    # B) Выбран элемент активной модели
    host_el = doc.GetElement(ref.ElementId)
    if host_el is not None:
        id_text = str(host_el.Id.IntegerValue)
        _select_only(host_el.Id)
        _zoom_to(host_el.Id)
        copied = _copy_to_clipboard(id_text)
        msg = u"Выбран элемент в активной модели.\nID: {0}".format(id_text)
        msg += u"\nID скопирован в буфер обмена." if copied else u"\nНе удалось скопировать ID в буфер обмена."
        _show(u"Выбор элемента", msg)
        return

    _show(u"Выбор элемента", u"Не удалось обработать выбор элемента.")


if __name__ == "__main__":
    main()
