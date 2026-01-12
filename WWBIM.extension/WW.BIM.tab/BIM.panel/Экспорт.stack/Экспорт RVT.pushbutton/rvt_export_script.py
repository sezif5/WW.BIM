# -*- coding: utf-8 -*-
__title__  = "Экспорт RVT"
__author__ = "vlad / you"
__doc__    = ("Открывает модели с Revit Server в фоне и сохраняет в локальную папку.\n"
              "Режимы: preserve (Detach&Preserve + SaveAsCentral), "
              "discard (Detach&Discard), none (без Detach).\n"
              "Открытие РН: все, кроме начинающихся с '00_' и содержащих 'Link'/'Связь'.\n"
              "Автоматическая обработка предупреждений Revit.")

import os, datetime
from pyrevit import script, coreutils, forms
import System.Collections.Generic

# Revit API
from Autodesk.Revit.DB import (
    ModelPathUtils, ModelPath, OpenOptions, DetachFromCentralOption,
    SaveAsOptions, WorksharingSaveAsOptions,
    FilteredElementCollector, RevitLinkType, PathType, Transaction,
    ImportInstance, BuiltInCategory, ElementId
)

# ваши модули
import openbg   # политика РН и фоновое открытие (использую _build_ws_config/open_in_background)  # noqa
import closebg  # корректное закрытие/синхронизация                                                    # noqa

# ---------------- настройки ----------------
DETACH_MODE        = "preserve"       # "preserve" | "discard" | "none"
COMPACT_ON_SAVE    = True
OVERWRITE_SAME     = True
# Фильтр рабочих наборов: исключаются начинающиеся с "00_" и содержащие "Link"/"Связь"

# ---------------- helpers ----------------
out = script.get_output()
out.close_others(all_open_outputs=True)

def to_model_path(user_visible_path):
    if not user_visible_path:
        return None
    try:
        return ModelPathUtils.ConvertUserVisiblePathToModelPath(user_visible_path)
    except Exception:
        return None

def default_export_root():
    root = os.path.join(os.path.expanduser("~"), "Documents", "RVT_Detached")
    if not os.path.exists(root):
        try: os.makedirs(root)
        except Exception: pass
    return root

def select_export_root():
    folder = forms.pick_folder(title=u"Куда сохранить RVT")
    if not folder:
        folder = default_export_root()
        out.print_md(u":information_source: Папка не выбрана. Используем: `{}`".format(folder))
    return os.path.normpath(folder)

# ---------------- преобразование путей связей ----------------
def update_links_to_export_folder(saved_file_path, export_folder):
    """
    Обновляет пути всех связанных файлов (RevitLink) так, чтобы они указывали
    на файлы в папке экспорта. Пути становятся относительными.
    Вызывается ПОСЛЕ сохранения файла.
    Возвращает количество обновлённых связей.
    """
    from Autodesk.Revit.DB import TransmissionData, ExternalFileReferenceType
    
    updated = 0
    
    try:
        # Получаем ModelPath сохранённого файла
        mp = ModelPathUtils.ConvertUserVisiblePathToModelPath(saved_file_path)
        
        # Читаем TransmissionData (метаданные о связях без открытия документа)
        trans_data = TransmissionData.ReadTransmissionData(mp)
        if trans_data is None:
            return 0
        
        # Получаем все внешние ссылки
        ext_refs = trans_data.GetAllExternalFileReferenceIds()
        
        for ref_id in ext_refs:
            try:
                ext_ref = trans_data.GetLastSavedReferenceData(ref_id)
                if ext_ref is None:
                    continue
                
                # Проверяем тип ссылки (нас интересуют только RevitLink)
                ref_type = ext_ref.ExternalFileReferenceType
                if ref_type != ExternalFileReferenceType.RevitLink:
                    continue
                
                # Получаем текущий путь к связи
                abs_path = ext_ref.GetAbsolutePath()
                if abs_path is None:
                    continue
                
                # Получаем имя файла связи
                old_path_str = ModelPathUtils.ConvertModelPathToUserVisiblePath(abs_path)
                link_filename = os.path.basename(old_path_str)
                
                # Формируем новый путь в папке экспорта
                new_path_str = os.path.join(export_folder, link_filename)
                new_model_path = ModelPathUtils.ConvertUserVisiblePathToModelPath(new_path_str)
                
                # Устанавливаем новый путь (относительный)
                trans_data.SetDesiredReferenceData(ref_id, new_model_path, PathType.Relative, True)
                updated += 1
                
            except Exception as e:
                out.print_md(u"  :warning: Ошибка обработки связи: {}".format(e))
        
        # Записываем изменённые TransmissionData обратно в файл
        if updated > 0:
            trans_data.IsTransmitted = True
            TransmissionData.WriteTransmissionData(mp, trans_data)
            
    except Exception as e:
        out.print_md(u":x: Ошибка обновления путей связей: {}".format(e))
    
    return updated

# ---------------- очистка от неиспользуемых элементов ----------------
def get_all_purgeable_ids(doc):
    """
    Получает все неиспользуемые элементы через PerformanceAdviser.
    Возвращает set ElementId.
    """
    from Autodesk.Revit.DB import PerformanceAdviser, PerformanceAdviserRuleId
    from System.Collections.Generic import List
    
    purgeable = set()
    
    try:
        adviser = PerformanceAdviser.GetPerformanceAdviser()
        all_rule_ids = adviser.GetAllRuleIds()
        
        purge_guid = "e8c63650-70b7-435a-9010-ec97660c1bda"
        purge_rule_id = None
        
        for rule_id in all_rule_ids:
            if str(rule_id.Guid) == purge_guid:
                purge_rule_id = rule_id
                break
        
        if purge_rule_id is None:
            for rule_id in all_rule_ids:
                name = adviser.GetRuleName(rule_id)
                if "purgeable" in name.lower() or "unused" in name.lower():
                    purge_rule_id = rule_id
                    break
        
        if purge_rule_id:
            rule_list = List[PerformanceAdviserRuleId]()
            rule_list.Add(purge_rule_id)
            failure_messages = adviser.ExecuteRules(doc, rule_list)
            
            if failure_messages:
                for failure_msg in failure_messages:
                    elem_ids = failure_msg.GetFailingElements()
                    if elem_ids:
                        for eid in elem_ids:
                            purgeable.add(eid)
    except:
        pass
    
    return purgeable

def purge_unused(doc):
    """
    Очищает модель от неиспользуемых элементов.
    Выполняет множественные проходы до полной очистки.
    Возвращает общее количество удалённых элементов.
    """
    from Autodesk.Revit.DB import PerformanceAdviser, PerformanceAdviserRuleId
    from System.Collections.Generic import List
    
    total_purged = 0
    max_passes = 20
    
    # Ищем правило для очистки один раз
    purge_guid = "e8c63650-70b7-435a-9010-ec97660c1bda"
    purge_rule_id = None
    
    try:
        adviser = PerformanceAdviser.GetPerformanceAdviser()
        all_rule_ids = adviser.GetAllRuleIds()
        
        for rule_id in all_rule_ids:
            if str(rule_id.Guid) == purge_guid:
                purge_rule_id = rule_id
                break
        
        if purge_rule_id is None:
            for rule_id in all_rule_ids:
                name = adviser.GetRuleName(rule_id)
                if "purgeable" in name.lower() or "unused" in name.lower():
                    purge_rule_id = rule_id
                    break
        
        if purge_rule_id is None:
            out.print_md(u"  :warning: Правило очистки не найдено")
            return 0
    except Exception as e:
        out.print_md(u"  :warning: Ошибка поиска правила: {}".format(e))
        return 0
    
    for pass_num in range(max_passes):
        try:
            # Получаем adviser
            adviser = PerformanceAdviser.GetPerformanceAdviser()
            
            # Создаём список с правилом
            rule_list = List[PerformanceAdviserRuleId]()
            rule_list.Add(purge_rule_id)
            
            # Выполняем правило
            failure_messages = adviser.ExecuteRules(doc, rule_list)
            
            # Собираем ВСЕ элементы
            all_ids_to_delete = set()
            
            if failure_messages:
                for failure_msg in failure_messages:
                    elem_ids = failure_msg.GetFailingElements()
                    if elem_ids:
                        for eid in elem_ids:
                            all_ids_to_delete.add(eid)
            
            if not all_ids_to_delete:
                # Нет элементов для удаления - заканчиваем
                break
            
            # Удаляем все найденные элементы
            purged_this_pass = 0
            ids_list = list(all_ids_to_delete)
            
            t = Transaction(doc, "Purge Unused - Pass {}".format(pass_num + 1))
            t.Start()
            try:
                for eid in ids_list:
                    try:
                        doc.Delete(eid)
                        purged_this_pass += 1
                    except:
                        pass
                # Регенерируем модель внутри транзакции
                doc.Regenerate()
                t.Commit()
            except Exception as ex:
                t.RollBack()
                out.print_md(u"    :warning: Ошибка в проходе {}: {}".format(pass_num + 1, ex))
                break
            
            if purged_this_pass > 0:
                total_purged += purged_this_pass
                out.print_md(u"    Проход {}: удалено **{}** элементов".format(pass_num + 1, purged_this_pass))
            else:
                # Ничего не удалили - выходим
                break
                
        except Exception as e:
            out.print_md(u"  :warning: Ошибка в проходе {}: {}".format(pass_num + 1, e))
            break
    
    return total_purged

# ---------------- удаление CAD импортов ----------------
def delete_cad_imports(doc):
    """Удаляет все CAD импорты (вставленные DWG/DXF)."""
    deleted = 0
    try:
        cad_imports = FilteredElementCollector(doc).OfClass(ImportInstance).ToElements()
        if not cad_imports:
            return 0
        
        t = Transaction(doc, "Delete CAD Imports")
        t.Start()
        try:
            for elem in cad_imports:
                # Проверяем что это импорт, а не связь
                if not elem.IsLinked:
                    try:
                        doc.Delete(elem.Id)
                        deleted += 1
                    except:
                        pass
            t.Commit()
        except:
            t.RollBack()
    except Exception as e:
        out.print_md(u"  :warning: Ошибка удаления CAD импортов: {}".format(e))
    return deleted

# ---------------- удаление CAD связей ----------------
def delete_cad_links(doc):
    """Удаляет все CAD связи (связанные DWG/DXF)."""
    deleted = 0
    try:
        # Удаляем экземпляры связей
        cad_imports = FilteredElementCollector(doc).OfClass(ImportInstance).ToElements()
        linked_ids = [elem.Id for elem in cad_imports if elem.IsLinked]
        
        # Также удаляем типы CAD связей
        try:
            from Autodesk.Revit.DB import CADLinkType
            cad_types = FilteredElementCollector(doc).OfClass(CADLinkType).ToElements()
            for ct in cad_types:
                linked_ids.append(ct.Id)
        except:
            pass
        
        if not linked_ids:
            return 0
        
        t = Transaction(doc, "Delete CAD Links")
        t.Start()
        try:
            for eid in linked_ids:
                try:
                    doc.Delete(eid)
                    deleted += 1
                except:
                    pass
            t.Commit()
        except:
            t.RollBack()
    except Exception as e:
        out.print_md(u"  :warning: Ошибка удаления CAD связей: {}".format(e))
    return deleted

# ---------------- удаление растровых изображений ----------------
def delete_raster_images(doc):
    """Удаляет все растровые изображения."""
    deleted = 0
    ids_to_delete = []
    
    try:
        # ImageInstance - экземпляры изображений
        try:
            from Autodesk.Revit.DB import ImageInstance
            images = FilteredElementCollector(doc).OfClass(ImageInstance).ToElements()
            for elem in images:
                ids_to_delete.append(elem.Id)
        except:
            pass
        
        # ImageType - типы изображений
        try:
            from Autodesk.Revit.DB import ImageType
            image_types = FilteredElementCollector(doc).OfClass(ImageType).ToElements()
            for elem in image_types:
                ids_to_delete.append(elem.Id)
        except:
            pass
        
        if not ids_to_delete:
            return 0
        
        t = Transaction(doc, "Delete Raster Images")
        t.Start()
        try:
            for eid in ids_to_delete:
                try:
                    doc.Delete(eid)
                    deleted += 1
                except:
                    pass
            t.Commit()
        except:
            t.RollBack()
    except Exception as e:
        out.print_md(u"  :warning: Ошибка удаления изображений: {}".format(e))
    return deleted

# ---------------- удаление 2D-подложек (устаревшая, для совместимости) ----------------
def delete_2d_underlays(doc, settings=None):
    """
    Удаляет 2D-подложки в зависимости от настроек.
    settings - словарь с ключами: 'cad_imports', 'cad_links', 'raster_images'
    """
    if settings is None:
        settings = {'cad_imports': True, 'cad_links': True, 'raster_images': True}
    
    total = 0
    
    if settings.get('cad_imports', False):
        count = delete_cad_imports(doc)
        if count > 0:
            out.print_md(u"    CAD импорты: **{}**".format(count))
        total += count
    
    if settings.get('cad_links', False):
        count = delete_cad_links(doc)
        if count > 0:
            out.print_md(u"    CAD связи: **{}**".format(count))
        total += count
    
    if settings.get('raster_images', False):
        count = delete_raster_images(doc)
        if count > 0:
            out.print_md(u"    Растровые изображения: **{}**".format(count))
        total += count
    
    return total

def pick_models():
    # сперва пробуем твой кастомный селектор (как в экспортёре NWC)
    try:
        from sup import select_file as pick_models_custom
        models = pick_models_custom() or []
        return list(models)
    except Exception:
        pass
    models = forms.pick_file(files_filter="Revit files (*.rvt)|*.rvt",
                             multi_file=True,
                             title="Выберите Revit-модели (можно RSN://)")
    return list(models) if models else []

# ---------------- диалог настроек очистки ----------------
class CleanupOption:
    """Класс для опции очистки в диалоге выбора."""
    def __init__(self, name, key, default=True):
        self.name = name
        self.key = key
        self.default = default
    
    def __repr__(self):
        return self.name

def select_cleanup_options():
    """
    Показывает диалог выбора опций очистки.
    Возвращает словарь с выбранными опциями.
    """
    options = [
        CleanupOption(u"Удалять CAD импорты", "cad_imports", True),
        CleanupOption(u"Удалять CAD связи", "cad_links", True),
        CleanupOption(u"Удалять растровые изображения", "raster_images", True),
        CleanupOption(u"Очищать неиспользуемые элементы (Purge)", "purge_unused", True),
    ]
    
    selected = forms.SelectFromList.show(
        options,
        title=u"Настройки очистки модели",
        width=450,
        height=300,
        button_name=u"Продолжить",
        multiselect=True
    )
    
    # Формируем словарь результатов
    result = {opt.key: False for opt in options}
    
    if selected:
        for opt in selected:
            result[opt.key] = True
    
    return result

def model_name_from_path(p):
    try: return os.path.basename(p)
    except Exception: return "Model.rvt"

# ---------------- открытие ----------------
def is_workshared_file(mp):
    """Проверяет, является ли файл workshared (без открытия)."""
    try:
        from Autodesk.Revit.DB import BasicFileInfo
        file_info = BasicFileInfo.Extract(ModelPathUtils.ConvertModelPathToUserVisiblePath(mp))
        return file_info.IsWorkshared
    except Exception:
        # Если не удалось определить — считаем что workshared (безопаснее)
        return True

def get_workset_filter():
    """Возвращает функцию-предикат для фильтрации рабочих наборов"""
    def workset_filter(ws_name):
        """Возвращает True, если рабочий набор нужно открыть"""
        name = (ws_name or u"").strip()
        # Исключаем: начинающиеся с '00_'
        if name.startswith(u'00_'):
            return False
        # Исключаем: содержащие 'Link' или 'Связь' (регистронезависимо)
        name_lower = name.lower()
        if u'link' in name_lower or u'связь' in name_lower:
            return False
        return True
    return workset_filter

def open_document(mp, worksets_rule):
    """
    Открывает документ с автоматической обработкой предупреждений.
    Возвращает кортеж (doc, failure_handler).
    """
    app = __revit__.Application
    ui  = __revit__

    # Проверяем, является ли файл workshared
    workshared = is_workshared_file(mp)

    if not workshared:
        # Для не-workshared файлов — используем openbg без detach
        out.print_md(u"  :information_source: Файл не является Workshared")
        return openbg.open_in_background(app, ui, mp, audit=False, worksets=worksets_rule, detach=False, suppress_warnings=True)

    if DETACH_MODE == "preserve":
        # Используем openbg.open_in_background с detach=True
        return openbg.open_in_background(app, ui, mp, audit=False, worksets=worksets_rule, detach=True, suppress_warnings=True)

    if DETACH_MODE == "discard":
        # Для discard режима используем openbg с специальной настройкой
        # К сожалению, openbg.open_in_background поддерживает только DetachAndPreserveWorksets
        # Поэтому используем прямой вызов с ручным обработчиком предупреждений
        opts = OpenOptions()
        opts.Audit = False
        opts.DetachFromCentralOption = DetachFromCentralOption.DetachAndDiscardWorksets

        # Создаем обработчик предупреждений вручную
        failure_handler = openbg.SuppressWarningsPreprocessor()
        try:
            app.FailuresProcessing += failure_handler.PreprocessFailures
        except Exception:
            pass

        try:
            doc = app.OpenDocumentFile(mp, opts)
            return (doc, failure_handler)
        finally:
            try:
                app.FailuresProcessing -= failure_handler.PreprocessFailures
            except Exception:
                pass

    # DETACH_MODE == "none"
    return openbg.open_in_background(app, ui, mp, audit=False, worksets=worksets_rule, detach=False, suppress_warnings=True)

# ---------------- сохранение ----------------
def save_document(doc, full_path):
    # гарантируем наличие директории
    dst_dir = os.path.dirname(full_path)
    if not os.path.exists(dst_dir):
        try: os.makedirs(dst_dir)
        except Exception: pass

    # Лучше использовать ModelPath (работает и с локальными путями)
    mp_out = ModelPathUtils.ConvertUserVisiblePathToModelPath(full_path)

    sao = SaveAsOptions()
    sao.Compact = bool(COMPACT_ON_SAVE)
    sao.OverwriteExistingFile = bool(OVERWRITE_SAME)

    # Проверяем, является ли документ workshared
    is_workshared = doc.IsWorkshared
    
    if DETACH_MODE == "preserve" and is_workshared:
        # ВКЛЮЧИТЬ worksharing-опции ВНУТРЬ SaveAsOptions
        wsa = WorksharingSaveAsOptions()
        wsa.SaveAsCentral = True  # <-- ключ к сохранению «с сохранением РН»
        sao.SetWorksharingOptions(wsa)  # <-- правильный способ передать WSA
        doc.SaveAs(mp_out, sao)

    else:
        # discard/none или не-workshared -> обычный файл
        doc.SaveAs(mp_out, sao)

# ---------------- main ----------------
def main():
    sel_models = pick_models()
    if not sel_models:
        script.exit()

    export_root = select_export_root()
    
    # Диалог выбора опций очистки
    cleanup_settings = select_cleanup_options()
    if cleanup_settings is None:
        script.exit()

    out.print_md("## Сохранение RVT ({} шт.)".format(len(sel_models)))
    out.print_md("Папка: **{}**".format(export_root))
    out.print_md("Режим Detach: `{}`; Compact: {}; Overwrite: {}"
                 .format(DETACH_MODE, COMPACT_ON_SAVE, OVERWRITE_SAME))
    out.print_md(u"РН: исключаются `00_*`, `*Link*`, `*Связь*`")
    
    # Выводим выбранные настройки очистки
    cleanup_info = []
    if cleanup_settings.get('cad_imports'): cleanup_info.append(u"CAD импорты")
    if cleanup_settings.get('cad_links'): cleanup_info.append(u"CAD связи")
    if cleanup_settings.get('raster_images'): cleanup_info.append(u"Растровые изображения")
    if cleanup_settings.get('purge_unused'): cleanup_info.append(u"Purge Unused")
    
    if cleanup_info:
        out.print_md(u"Очистка: **{}**".format(u", ".join(cleanup_info)))
    else:
        out.print_md(u"Очистка: *отключена*")
    out.print_md("___")

    total_timer = coreutils.Timer()
    out.update_progress(0, len(sel_models))

    for i, user_path in enumerate(sel_models):
        model_file  = model_name_from_path(user_path)
        name_wo_ext = os.path.splitext(model_file)[0]
        dst_file    = os.path.join(export_root, name_wo_ext + ".rvt")

        out.print_md(u":open_file_folder: **{}** → {}".format(model_file, dst_file))

        mp = to_model_path(user_path)
        if mp is None:
            out.print_md(":x: Не удалось преобразовать путь в ModelPath. Пропуск.")
            out.update_progress(i + 1, len(sel_models)); continue

        # Открытие
        t_open = coreutils.Timer()

        # Используем фильтр рабочих наборов (исключаем 00_, Link, Связь)
        workset_rule = ('predicate', get_workset_filter())

        try:
            doc, failure_handler = open_document(mp, workset_rule)
        except Exception as e:
            out.print_md(":x: Ошибка открытия: `{}`".format(e))
            out.update_progress(i + 1, len(sel_models)); continue
        open_s = str(datetime.timedelta(seconds=int(t_open.get_time())))

        # Вывод информации об обработанных предупреждениях/ошибках
        if failure_handler is not None:
            try:
                summary = failure_handler.get_summary()
                total_w = summary.get('total_warnings', 0)
                total_e = summary.get('total_errors', 0)
                if total_w > 0 or total_e > 0:
                    out.print_md(u"  :warning: При открытии обработано автоматически: **{} предупреждений, {} ошибок**".format(total_w, total_e))
                    # Вывод первых 3 предупреждений (короче, чем в NWC скрипте)
                    if total_w > 0:
                        warnings = summary.get('warnings', [])
                        for idx, w in enumerate(warnings[:3], 1):
                            out.print_md(u"    {}. {}".format(idx, w))
                        if total_w > 3:
                            out.print_md(u"    ... и ещё {} предупреждений".format(total_w - 3))
                    # Вывод первых 2 ошибок
                    if total_e > 0:
                        errors = summary.get('errors', [])
                        for idx, err in enumerate(errors[:2], 1):
                            out.print_md(u"    Ошибка {}: {}".format(idx, err))
                        if total_e > 2:
                            out.print_md(u"    ... и ещё {} ошибок".format(total_e - 2))
            except Exception:
                pass

        # Удаление 2D-подложек (CAD импорты, CAD связи, изображения)
        any_cleanup = cleanup_settings.get('cad_imports') or cleanup_settings.get('cad_links') or cleanup_settings.get('raster_images')
        if any_cleanup:
            try:
                underlays_deleted = delete_2d_underlays(doc, cleanup_settings)
                if underlays_deleted > 0:
                    out.print_md(u"  :wastebasket: Удалено подложек: **{}**".format(underlays_deleted))
            except Exception as e:
                out.print_md(u"  :warning: Ошибка удаления подложек: {}".format(e))

        # Очистка от неиспользуемых компонентов (Purge Unused)
        if cleanup_settings.get('purge_unused'):
            try:
                purged_count = purge_unused(doc)
                if purged_count > 0:
                    out.print_md(u"  :broom: Очищено неиспользуемых элементов: **{}**".format(purged_count))
            except Exception as e:
                out.print_md(u"  :warning: Ошибка очистки: {}".format(e))

        # Сохранение
        t_save = coreutils.Timer()
        ok, err = True, None
        try:
            save_document(doc, dst_file)
        except Exception as e:
            ok, err = False, str(e)
        save_s = str(datetime.timedelta(seconds=int(t_save.get_time())))

        # Закрытие (без sync на сервер)
        try:
            closebg.close_with_policy(doc, do_sync=False, save_if_not_ws=False)
        except Exception:
            pass

        # Обновление путей связей на папку экспорта (после сохранения и закрытия)
        if ok and os.path.exists(dst_file):
            try:
                links_updated = update_links_to_export_folder(dst_file, export_root)
                if links_updated > 0:
                    out.print_md(u"  :link: Обновлено путей связей на папку экспорта: **{}**".format(links_updated))
            except Exception as e:
                out.print_md(u"  :warning: Ошибка обновления путей связей: {}".format(e))

        outcome = u":white_check_mark: OK" if ok else u":x: Ошибка — {}".format(err)
        out.print_md(u"- Открытие: **{}**, Сохранение: **{}** → {}".format(open_s, save_s, outcome))
        if ok and os.path.exists(dst_file):
            out.print_md(u"Готово: `{}`".format(dst_file))
        out.print_md("___")
        out.update_progress(i + 1, len(sel_models))

    all_s = str(datetime.timedelta(seconds=int(total_timer.get_time())))
    out.print_md("**Готово. Время всего: {}**".format(all_s))

if __name__ == "__main__":
    main()
