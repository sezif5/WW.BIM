# -*- coding: utf-8 -*-
"""Update WW.BIM plugin from GitHub (without version.txt)"""

__title__ = "Update\nplugin"
__author__ = "WW.BIM"
__doc__ = "Downloads latest plugin version from GitHub"

import os
import sys
import shutil
import zipfile
import json
import traceback
from datetime import datetime

from pyrevit import script, forms

try:
    from System.Net import WebClient, ServicePointManager, SecurityProtocolType

    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
except Exception:
    WebClient = None

GITHUB_USER = "sezif5"
GITHUB_REPO = "WWBIM.extension"
BRANCH = "main"
EXTENSION_NAME = "WW.BIM.extension"


def http_get_text(url):
    if WebClient is None:
        return None

    client = WebClient()
    client.Headers.Add("User-Agent", "WW.BIM-Update/1.0")
    return client.DownloadString(url)


def http_download_file(url, path):
    if WebClient is None:
        return False

    client = WebClient()
    client.Headers.Add("User-Agent", "WW.BIM-Update/1.0")
    client.DownloadFile(url, path)
    return True


def get_extension_path():
    script_path = os.path.dirname(__file__)
    ext_path = script_path

    for _ in range(10):
        if ext_path.endswith(".extension"):
            return ext_path
        parent = os.path.dirname(ext_path)
        if parent == ext_path:
            break
        ext_path = parent

    return None


def get_local_last_update(ext_path):
    last_update_file = os.path.join(ext_path, "last_update.txt")
    if os.path.exists(last_update_file):
        with open(last_update_file, "r") as f:
            return f.read().strip()
    return None


def get_remote_commit_date():
    for branch in [BRANCH, "master", "main"]:
        url = "https://api.github.com/repos/{}/{}/commits/{}".format(
            GITHUB_USER, GITHUB_REPO, branch
        )
        try:
            response_text = http_get_text(url)
            if not response_text:
                continue
            data = json.loads(response_text)
            commit_date = data["commit"]["committer"]["date"]
            return commit_date
        except Exception:
            continue
    return None


def compare_dates(remote_date, local_date):
    if not local_date:
        return True

    try:
        remote = datetime.strptime(remote_date, "%Y-%m-%dT%H:%M:%SZ")
        local = datetime.strptime(local_date, "%Y-%m-%dT%H:%M:%SZ")
        return remote > local
    except:
        return True


def save_last_update(ext_path, date):
    last_update_file = os.path.join(ext_path, "last_update.txt")
    with open(last_update_file, "w") as f:
        f.write(date)


def get_temp_dir():
    system_drive = os.environ.get("SystemDrive", "C:\\")
    if sys.platform.startswith("win"):
        temp_dir = os.path.join(system_drive, "WWBIM_TMP")
    else:
        temp_dir = "/tmp/wwbim_update"

    try:
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
    except Exception:
        pass

    return temp_dir


def download_update(ext_path):
    temp_dir = get_temp_dir()

    if not os.path.exists(temp_dir):
        return False, "Temp directory does not exist"

    zip_path = os.path.join(temp_dir, "wwbim_update.zip")
    extract_path = os.path.join(temp_dir, "wwbim_update")

    for branch in [BRANCH, "master", "main"]:
        zip_url = "https://github.com/{}/{}/archive/refs/heads/{}.zip".format(
            GITHUB_USER, GITHUB_REPO, branch
        )
        try:
            if http_download_file(zip_url, zip_path):
                used_branch = branch
                break
        except Exception:
            continue
    else:
        return False, "Failed to download. Check internet connection."

    try:
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

        extracted_folder = os.path.join(
            extract_path, "{}-{}".format(GITHUB_REPO, used_branch)
        )

        source_ext = None
        possible_ext = os.path.join(extracted_folder, EXTENSION_NAME)
        if os.path.exists(possible_ext):
            source_ext = possible_ext
        else:
            for item in os.listdir(extracted_folder):
                if item.endswith(".extension"):
                    source_ext = os.path.join(extracted_folder, item)
                    break

        if not source_ext:
            source_ext = extracted_folder

        skip_files = [
            "user_config.json",
            "local_settings.py",
            ".user",
            "last_update.txt",
        ]

        for item in os.listdir(source_ext):
            if item in skip_files:
                continue
            if item.startswith(".git"):
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
                pass

        try:
            os.remove(zip_path)
            shutil.rmtree(extract_path)
        except:
            pass

        return True, "Update installed successfully!"

    except Exception as e:
        return False, "Installation error: {}".format(str(e))


def main():
    ext_path = get_extension_path()

    if not ext_path:
        ext_path = forms.pick_folder(title="Select plugin folder (.extension)")
        if not ext_path:
            script.exit()

    if WebClient is None:
        forms.alert("WebClient not available. Update aborted.", warn_icon=True)
        script.exit()

    local_date = get_local_last_update(ext_path)
    remote_date = get_remote_commit_date()

    if not remote_date:
        if forms.alert(
            "Failed to check commit date on server.\n\n"
            "Last update: {}\n\n"
            "Force update?".format(local_date or "unknown"),
            yes=True,
            no=True,
        ):
            success, message = download_update(ext_path)
            if success:
                remote_date = get_remote_commit_date()
                if remote_date:
                    save_last_update(ext_path, remote_date)
                forms.alert("{}\n\nRestart Revit to apply changes.".format(message))
            else:
                forms.alert(message, warn_icon=True)
        script.exit()

    if not compare_dates(remote_date, local_date):
        result = forms.alert(
            "You have the latest version\n\nLast update: {}\n\nForce update?".format(
                local_date or "unknown"
            ),
            yes=True,
            no=True,
        )
        if not result:
            script.exit()
    else:
        result = forms.alert(
            "Update available!\n\nLatest commit date: {}\n\nInstall update?".format(
                remote_date
            ),
            yes=True,
            no=True,
        )
        if not result:
            script.exit()

    success, message = download_update(ext_path)

    if success:
        save_last_update(ext_path, remote_date)
        forms.alert("{}\n\nRestart Revit to apply changes.".format(message))
    else:
        forms.alert(message, warn_icon=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        forms.alert(
            "Update failed.\n\n{}".format(traceback.format_exc()),
            warn_icon=True,
        )
        script.exit()
