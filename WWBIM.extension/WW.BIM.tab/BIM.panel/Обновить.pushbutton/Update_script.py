# -*- coding: utf-8 -*-
"""Обновить плагин WW.BIM из GitHub"""
__title__ = "Обновить\nплагин"
__author__ = "WW.BIM"
__doc__ = "Скачивает последнюю версию плагина с GitHub"

import os
import sys
import shutil
import zipfile

from pyrevit import script, forms

# Python 2/3 совместимость
if sys.version_info[0] >= 3:
    from urllib.request import urlretrieve, urlopen
else:
    from urllib import urlretrieve
    from urllib2 import urlopen

# ============ НАСТРОЙКИ ============
GITHUB_USER = "sezif5"
GITHUB_REPO = "WW.BIM"
BRANCH = "main"  # или "master" - проверьте в вашем репо
# Название папки extension (измените если отличается)
EXTENSION_NAME = "WW.BIM.extension"
# ===================================


def get_extension_path():
    """Получаем путь к папке extension"""
    script_path = os.path.dirname(__file__)
    ext_path = script_path

    # Поднимаемся вверх пока не найдем .extension
    for _ in range(10):
        if ext_path.endswith('.extension'):
            return ext_path
        parent = os.path.dirname(ext_path)
        if parent == ext_path:
            break
        ext_path = parent

    return None


def get_local_version(ext_path):
    """Читаем локальную версию из version.txt"""
    version_file = os.path.join(ext_path, 'version.txt')
    if os.path.exists(version_file):
        with open(version_file, 'r') as f:
            return f.read().strip()
    return "0.0.0"


def get_remote_version():
    """Получаем версию с GitHub"""
    url = "https://raw.githubusercontent.com/{}/{}/{}/version.txt".format(
        GITHUB_USER, GITHUB_REPO, BRANCH
    )
    try:
        response = urlopen(url, timeout=10)
        return response.read().decode('utf-8').strip()
    except:
        # Пробуем ветку master если main не сработал
        try:
            url = url.replace('/main/', '/master/')
            response = urlopen(url, timeout=10)
            return response.read().decode('utf-8').strip()
        except:
            return None


def download_update(ext_path):
    """Скачиваем и устанавливаем обновление"""
    temp_dir = os.environ.get('TEMP', os.environ.get('TMP', '/tmp'))
    zip_path = os.path.join(temp_dir, 'wwbim_update.zip')
    extract_path = os.path.join(temp_dir, 'wwbim_update')

    # Пробуем main, потом master
    for branch in [BRANCH, 'master', 'main']:
        zip_url = "https://github.com/{}/{}/archive/refs/heads/{}.zip".format(
            GITHUB_USER, GITHUB_REPO, branch
        )
        try:
            urlretrieve(zip_url, zip_path)
            used_branch = branch
            break
        except:
            continue
    else:
        return False, "Не удалось скачать обновление. Проверьте интернет."

    try:
        # Очищаем старую временную папку
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)

        # Распаковываем ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        # GitHub создает папку repo-branch
        extracted_folder = os.path.join(
            extract_path,
            "{}-{}".format(GITHUB_REPO, used_branch)
        )

        # Ищем папку extension внутри архива
        source_ext = None

        # Вариант 1: extension в подпапке
        possible_ext = os.path.join(extracted_folder, EXTENSION_NAME)
        if os.path.exists(possible_ext):
            source_ext = possible_ext
        else:
            # Вариант 2: ищем любую папку .extension
            for item in os.listdir(extracted_folder):
                if item.endswith('.extension'):
                    source_ext = os.path.join(extracted_folder, item)
                    break

        # Вариант 3: extension в корне репозитория
        if not source_ext:
            source_ext = extracted_folder

        # Файлы которые не перезаписываем (пользовательские настройки)
        skip_files = ['user_config.json', 'local_settings.py', '.user']

        # Копируем файлы
        for item in os.listdir(source_ext):
            if item in skip_files:
                continue
            if item.startswith('.git'):
                continue

            src = os.path.join(source_ext, item)
            dst = os.path.join(ext_path, item)

            try:
                if os.path.isdir(src):
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
                else:
                    shutil.copy2(src, dst)
            except Exception as e:
                print("Не удалось скопировать {}: {}".format(item, e))

        # Очистка временных файлов
        try:
            os.remove(zip_path)
            shutil.rmtree(extract_path)
        except:
            pass

        return True, "Обновление успешно установлено!"

    except Exception as e:
        return False, "Ошибка при установке: {}".format(str(e))


def force_update(ext_path):
    """Принудительное обновление без проверки версии"""
    with forms.ProgressBar(title="Скачивание обновления...") as pb:
        pb.update_progress(20, 100)
        success, message = download_update(ext_path)
        pb.update_progress(100, 100)
    return success, message


# ============ ГЛАВНЫЙ КОД ============
if __name__ == '__main__':
    ext_path = get_extension_path()

    if not ext_path:
        # Если не удалось определить путь автоматически
        # Пользователь может указать вручную
        ext_path = forms.pick_folder(title="Выберите папку плагина (.extension)")
        if not ext_path:
            forms.alert("Путь к плагину не выбран", exitscript=True)

    local_ver = get_local_version(ext_path)
    remote_ver = get_remote_version()

    # Если не удалось получить версию с сервера
    if not remote_ver:
        if forms.alert(
            "Не удалось проверить версию на сервере.\n\n"
            "Текущая версия: {}\n\n"
            "Выполнить принудительное обновление?".format(local_ver),
            yes=True, no=True
        ):
            success, message = force_update(ext_path)
            if success:
                forms.alert(
                    "{}\n\nПерезапустите Revit для применения изменений.".format(message)
                )
            else:
                forms.alert(message, warn_icon=True)
        sys.exit()

    # Сравниваем версии
    if remote_ver <= local_ver:
        result = forms.alert(
            "У вас актуальная версия: {}\n\n"
            "Обновление не требуется.\n\n"
            "Выполнить принудительное обновление?".format(local_ver),
            yes=True, no=True
        )
        if not result:
            sys.exit()
    else:
        # Есть новая версия
        result = forms.alert(
            "Доступна новая версия!\n\n"
            "Текущая: {}\n"
            "Новая: {}\n\n"
            "Установить обновление?".format(local_ver, remote_ver),
            yes=True, no=True
        )
        if not result:
            sys.exit()

    # Выполняем обновление
    success, message = force_update(ext_path)

    if success:
        forms.alert(
            "{}\n\n"
            "Перезапустите Revit для применения изменений.".format(message)
        )
    else:
        forms.alert(message, warn_icon=True)
