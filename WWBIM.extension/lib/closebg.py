# -*- coding: utf-8 -*-
from Autodesk.Revit.DB import (
    TransactWithCentralOptions, SynchronizeWithCentralOptions,
    RelinquishOptions, SaveOptions
)

def close_with_policy(doc, do_sync=False, comment=u"", compact=True, relinquish=True, save_if_not_ws=True):
    """
    Если do_sync=True и документ совместный — SWC с параметрами.
    Иначе: для не-совместных при желании просто Save, и закрыть без сохранения.
    """
    try:
        if do_sync and doc.IsWorkshared:
            try:
                twc = TransactWithCentralOptions()
                swc = SynchronizeWithCentralOptions()
                swc.Comment = comment or u""
                swc.Compact = bool(compact)
                swc.SetRelinquishOptions(RelinquishOptions(bool(relinquish)))
                doc.SynchronizeWithCentral(twc, swc)
            except Exception:
                pass
        else:
            if save_if_not_ws and not doc.IsWorkshared:
                try: doc.Save(SaveOptions())
                except Exception: pass
        try: doc.Close(False)
        except Exception: pass
    except Exception:
        try: doc.Close(False)
        except Exception: pass
