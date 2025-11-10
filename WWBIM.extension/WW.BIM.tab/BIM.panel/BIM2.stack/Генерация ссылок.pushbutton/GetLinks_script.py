# -*- coding: utf-8 -*-
__title__ = 'Генерация ссылок'
__author__ = 'IliaNistratov / IronPython + RSN-aware'
__doc__ = """
Ищет модели на Revit Server (папки *.rvt) и локальных шарах, сохраняет RSN-ссылки в txt.
Shift+клик — открыть/создать base_lst.txt.
"""

import os, re, sys, time, io
from pyrevit import script, forms
from System.Diagnostics import Process
from System.IO import (FileStream, FileMode, FileAccess, FileShare,
                       StreamReader, Directory)
import System.Text as Text

out = script.get_output()
try: script.get_output().close_others(all_open_outputs=True)
except: pass
try: out.set_width(900)
except: pass

# --- НАСТРОЙКИ ---
# можно указать ПАПКУ или ПОЛНЫЙ ПУТЬ к файлу списка
PATH_MODEL_LIST = r"Z:\02_Библиотека\03_Dynamo\Scripts"
OUTPUT_TXT_DIR  = r"Z:\02_Библиотека\03_Dynamo\Scripts\txt"

# Годы Revit Server для автоподстановки при разворачивании RSN -> UNC
RS_YEARS = ['2026','2025','2024','2023','2022','2021','2020']

IGNORE_DIRS_EXACT = set([
    u'_backup', u'backup', u'revit backup', u'revit_backup', u'revit_backups',
    u'архив', u'archive', u'old', u'temp', u'tmp', u'trash', u'корзина',
    u'etransmit', u'e-transmit', u'transmit', u'exports', u'export',
    u'$recycle.bin', u'system volume information'
])
IGNORE_DIRS_SUBSTR = [
    u'_backup', u'.backup', u'backup', u'архив', u'archive', u'temp', u'tmp',
    u'etransmit', u'e-transmit', u'trash', u'корзина'
]

# Папки-модели могут называться "Model.0001.rvt" (бэкап) — отсечём
RE_BACKUP_RVT = re.compile(r'\.\d{4}\.rvt$', re.IGNORECASE)
RE_RS_YEAR    = re.compile(r'\\revit\s*server\s*20\d{2}', re.IGNORECASE)
RE_RS_FOLDER  = re.compile(r'\\revitserver20\d{2}', re.IGNORECASE)
RE_PROJECTS   = re.compile(r'\\Projects', re.IGNORECASE)

def _lower(s):
    try: return s.lower()
    except: return s

def open_in_os(path):
    try: Process.Start(path)
    except Exception as e:
        out.print_md(u":x: Не удалось открыть **{0}**: {1}".format(path, e))

def _resolve_base_file(hint):
    if not hint: hint = os.getcwd()
    if os.path.isdir(hint):
        base_dir  = hint
        base_file = os.path.join(base_dir, 'base_lst.txt')
    else:
        base_file = hint
        base_dir  = os.path.dirname(hint) or os.getcwd()
        if not base_file.lower().endswith('.txt'):
            base_dir  = hint
            base_file = os.path.join(base_dir, 'base_lst.txt')
    if not os.path.exists(base_dir):
        try: os.makedirs(base_dir)
        except: pass
    if not os.path.exists(base_file):
        fs = FileStream(base_file, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite)
        sw = StreamWriter(fs, Text.Encoding.UTF8)
        try:
            sw.WriteLine(u"# По одному пути на строку (RSN или UNC). Примеры:")
            sw.WriteLine(u"# rsn:\\\\192.168.88.178\\0004_Ивакино 1.3")
            sw.WriteLine(u"# \\\\192.168.88.178\\Revit Server 2023\\Projects\\0004_Ивакино 1.3")
        finally:
            sw.Close(); fs.Close()
    return base_file

def read_base_list(hint):
    path = _resolve_base_file(hint)
    bases = []
    try:
        fs = FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
        sr = StreamReader(fs, Text.Encoding.UTF8, True)
        try:
            while not sr.EndOfStream:
                line = sr.ReadLine()
                if line is None: break
                s = line.strip()
                if not s or s.startswith(u'#'): continue
                bases.append(s)
        finally:
            sr.Close(); fs.Close()
    except Exception as e:
        out.print_md(u":x: Не удалось прочитать **{0}**: {1}".format(path, e))
    return bases, path

def should_skip_dir(name):
    dl = _lower(name)
    if dl in IGNORE_DIRS_EXACT: return True
    for sub in IGNORE_DIRS_SUBSTR:
        if sub in dl: return True
    return False

def rsn_to_unc(maybe_rsn):
    """Разворачивает rsn:\\host\\... в UNC \\host\\Revit Server {year}\\Projects\\... (ищет существующий)."""
    s = maybe_rsn.replace('/', '\\')
    if s.lower().startswith('rsn:\\'):
        tail = s[5:]  # после 'rsn:\'
        parts = [p for p in tail.split('\\') if p]
        if not parts:
            return r'\\'  # корень
        host = parts[0]
        rest = u'\\'.join(parts[1:])  # может начинаться с 'Projects\\...' или сразу 'Проект'
        # если пользователь уже вписал 'Revit Server 20xx\\Projects' — это уже UNC-хвост
        guess1 = u"\\\\" + host + u"\\" + rest
        if os.path.exists(guess1):
            return guess1
        # иначе попробуем типичные корни
        for y in RS_YEARS:
            guess = u"\\\\{0}\\Revit Server {1}\\Projects".format(host, y)
            candidate = guess if not rest else (guess + u"\\" + rest)
            if os.path.exists(candidate):
                return candidate
        # последнее fallback
        return u"\\\\" + host + (u"\\" + rest if rest else u"")
    return s  # это уже UNC/локальный путь

def to_rsn(link_unc):
    """UNC → RSN для сохранения (убираем 'Revit Server 20xx' и 'Projects')."""
    p = link_unc.replace('/', '\\')
    p = RE_RS_YEAR.sub('', p)
    p = RE_RS_FOLDER.sub('', p)
    p = RE_PROJECTS.sub('', p)
    p = p.lstrip('\\')
    return u'rsn:\\' + p

def find_models(start_path_unc):
    """Ищем И ФАЙЛЫ *.rvt, и ПАПКИ *.rvt (Revit Server)."""
    models, scanned = [], 0
    if not os.path.exists(start_path_unc):
        out.print_md(u":x: Путь не существует: **{0}**".format(start_path_unc))
        return models
    t0 = time.time()
    for root, dirs, files in os.walk(start_path_unc, topdown=True):
        # 1) поймаем папки-модели (*.rvt)
        found_dir_models = []
        for d in list(dirs):
            dl = d.lower()
            if should_skip_dir(d): 
                dirs.remove(d); 
                continue
            if dl.endswith('.rvt') and not RE_BACKUP_RVT.search(dl):
                full = os.path.join(root, d)
                models.append(full)
                found_dir_models.append(d)  # чтобы не углубляться внутрь модели
        # не спускаемся внутрь папок-моделей
        for d in found_dir_models:
            if d in dirs:
                dirs.remove(d)

        scanned += 1

        # 2) обычные файлы *.rvt (на локальных дисках)
        for fn in files:
            fl = fn.lower()
            if fl.endswith('.rvt') and not RE_BACKUP_RVT.search(fl):
                models.append(os.path.join(root, fn))

    dt = time.time() - t0
    out.print_md(u"- Просканировано папок: **{0}**, найдено моделей: **{1}** за {2:.2f} с".format(scanned, len(models), dt))
    # уникализируем и сортируем
    uniq = {}
    for p in models: uniq[_lower(p)] = p
    return [uniq[k] for k in sorted(uniq.keys())]

def save_txt(name_txt, unc_links):
    if not os.path.exists(OUTPUT_TXT_DIR):
        try: os.makedirs(OUTPUT_TXT_DIR)
        except: pass
    outpath = os.path.join(OUTPUT_TXT_DIR, name_txt)
    f = io.open(outpath, 'w', encoding='utf-8', errors='replace')
    try:
        for unc in unc_links:
            f.write(to_rsn(unc) + u'\n')
    finally:
        f.close()
    out.print_md(u":white_check_mark: txt сохранён: **{0}**".format(outpath))
    return outpath

# --- MAIN ---
try: shift = __shiftclick__
except: shift = False

bases, base_file = read_base_list(PATH_MODEL_LIST)

if shift:
    open_in_os(base_file); sys.exit(0)

if not bases:
    forms.alert(u"Список базовых путей пуст. Открою файл, добавьте строки и запустите снова.",
                title=u"Нет базовых путей")
    open_in_os(base_file)
    sys.exit(0)

selected = forms.SelectFromList.show(
    bases, title=u"Выбор базовой папки (где искать *.rvt)",
    width=900, height=700, button_name=u"Искать"
)
if not selected:
    out.print_md(u":grey_exclamation: Выбор отменён."); sys.exit(0)

start_unc = rsn_to_unc(selected)
out.print_md(u"База: **{0}**".format(selected))
out.print_md(u"Обхожу как UNC: **{0}**".format(start_unc))
out.print_md(u"__")

models_unc = find_models(start_unc)
if not models_unc:
    out.print_md(u":warning: Модели не найдены."); sys.exit(0)

out.print_md(u"Кол-во моделей: **{0}**".format(len(models_unc)))
if forms.alert(u"Список сформирован. Сохранить в txt?",
               options=[u"Да", u"Нет"], title=u"Сохранить результат?") == u"Да":
    fname = os.path.basename(start_unc.rstrip('\\/')) or u"models"
    outpath = save_txt(u"{0}.txt".format(fname), models_unc)
    open_in_os(os.path.dirname(outpath))
