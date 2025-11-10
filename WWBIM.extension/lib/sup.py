# -*- coding: utf-8 -*-
# sup.py — относительные пути к Scripts/Objects и base_lst.txt (PyRevit/IronPython совместимо)

from __future__ import print_function, unicode_literals
import os, sys

# --------- helpers ---------
def _norm(p):
    return os.path.normpath(os.path.abspath(p)) if p else p

def _module_dir():
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        import inspect
        mod = sys.modules.get(__name__)
        if mod is not None:
            return os.path.dirname(os.path.abspath(inspect.getfile(mod)))
        return os.getcwd()

def _find_scripts_root(start_dir):
    """Поднимаемся вверх и ищем папку 'Scripts' (ограничение по уровням, затем fallback)."""
    cur = _norm(start_dir)
    for _ in range(0, 8):
        if os.path.basename(cur).lower() == 'scripts':
            return cur
        parent = os.path.dirname(cur)
        if not parent or parent == cur:
            break
        cur = parent
    # типовая структура: lib -> WWBIM.extension -> Scripts
    return _norm(os.path.join(start_dir, os.pardir, os.pardir))

# --------- вычисление корня Scripts ---------
_env_root = os.environ.get('WW_SCRIPTS_ROOT')
if _env_root and os.path.isdir(_env_root):
    SCRIPTS_ROOT = _norm(_env_root)
else:
    SCRIPTS_ROOT = _find_scripts_root(_module_dir())

# --------- публичные пути (совместимы с прежним кодом) ---------
# РАНЬШЕ: Z:\02_Библиотека\03_Dynamo\Scripts\base_lst.txt
path_model = _norm(os.path.join(SCRIPTS_ROOT, 'base_lst.txt'))

# --------- прежняя функция select_file() с относительными путями ---------
# (используется вызывающим скриптом)
from pyrevit import script, forms

output = script.get_output()
script.get_output().close_others(all_open_outputs=True)
output.set_width(900)

user = __revit__.Application.Username
doc = __revit__.ActiveUIDocument.Document

def select_file():
    def list_files_in_folder(folder_path):
        lst_model = []
        try:
            files = os.listdir(folder_path)
            for file in files:
                # берём *.txt без расширения
                if file.lower().endswith('.txt'):
                    lst_model.append(file[:-4])
        except OSError as e:
            print(u"Ошибка чтения папки {}: {}".format(folder_path, e))
        return lst_model

    # РАНЬШЕ: Z:\02_Библиотека\03_Dynamo\Scripts\Objects
    folder_path = _norm(os.path.join(SCRIPTS_ROOT, 'Objects'))

    sel = list_files_in_folder(folder_path)
    if sel:
        selected_file = forms.SelectFromList.show(
            sel,
            title=u"Выбор объекта",
            width=400,
            button_name=u'Выбрать'
        )

        if selected_file:
            file_path = os.path.join(folder_path, selected_file)

            # читаем содержимое выбранного *.txt
            lst_model_project = []
            try:
                # бинарный режим + универсальная декодировка (IronPython/CPython)
                with open(file_path + ".txt", 'rb') as f:
                    for raw in f:
                        try:
                            line = raw.decode('utf-8').strip()
                        except Exception:
                            # на случай, если уже str (CPython3) или другая кодировка
                            try:
                                line = raw.decode('cp1251').strip()
                            except Exception:
                                line = raw.strip()
                        if line:
                            lst_model_project.append(line)
            except OSError as e:
                print(u"Ошибка при чтении файла {}: {}".format(file_path, e))

            with forms.WarningBar(title=doc.Title.split("_" + user)[0]):
                items = forms.SelectFromList.show(
                    lst_model_project,
                    title=u'Выбор раздела',
                    multiselect=True,
                    button_name=u'Выбрать',
                    width=800,
                    height=800
                )

            if items:
                return items
            else:
                forms.alert(u'Ничего не выбрано!', ok=False, exitscript=True)
    else:
        # вместо молчаливого выхода — понятная диагностика пути
        forms.alert(
            u"Папка с объектами пуста или недоступна:\n{}\n\n"
            u"Текущий SCRIPTS_ROOT:\n{}".format(folder_path, SCRIPTS_ROOT),
            ok=False, exitscript=True
        )

# Дополнительно (по желанию): функция для быстрой диагностики
def info():
    return {
        'SCRIPTS_ROOT': SCRIPTS_ROOT,
        'path_model': path_model,
        'objects_dir': _norm(os.path.join(SCRIPTS_ROOT, 'Objects')),
        'exists': {
            'SCRIPTS_ROOT': os.path.isdir(SCRIPTS_ROOT),
            'Objects': os.path.isdir(_norm(os.path.join(SCRIPTS_ROOT, 'Objects'))),
            'base_lst.txt': os.path.isfile(path_model),
        }
    }

if __name__ == '__main__':
    print(u'Диагностика:', info())
