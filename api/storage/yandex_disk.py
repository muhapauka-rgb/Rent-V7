import os
import requests
from datetime import datetime
from typing import Tuple

BASE_URL = os.getenv("YANDEX_WEBDAV_BASE_URL")
USERNAME = os.getenv("YANDEX_WEBDAV_USERNAME")
PASSWORD = os.getenv("YANDEX_WEBDAV_PASSWORD")
ROOT = os.getenv("YANDEX_STORAGE_ROOT", "tenants")


class YandexDiskError(Exception):
    pass


def _auth():
    return (USERNAME, PASSWORD)


def ensure_dir(path: str):
    url = f"{BASE_URL}/{path}"
    r = requests.request("MKCOL", url, auth=_auth())
    if r.status_code not in (201, 405):
        raise YandexDiskError(f"MKCOL failed {r.status_code}: {r.text}")


def upload_bytes(
    chat_id: int,
    filename: str,
    content: bytes,
    category: str = "photo"
) -> str:
    """
    Uploads file bytes to Yandex Disk.
    Returns full disk path.
    """
    ts = datetime.utcnow().strftime("%Y.%m.%d-%H%M%S")
    safe_name = filename or f"{category}.jpg"

    dir_path = f"{ROOT}/{chat_id}"
    ensure_dir(ROOT)
    ensure_dir(dir_path)

    disk_path = f"{dir_path}/{ts}.{category}.{safe_name}"
    url = f"{BASE_URL}/{disk_path}"

    r = requests.put(
        url,
        data=content,
        auth=_auth(),
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    if r.status_code not in (201, 204):
        raise YandexDiskError(f"Upload failed {r.status_code}: {r.text}")

    return disk_path
