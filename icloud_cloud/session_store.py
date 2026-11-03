"""iCloud-Sitzung <-> AWS Secrets Manager (staubsauger#2).

pyicloud legt eine Sitzung als Dateien in einem Cookie-Verzeichnis ab
(<user>.cookiejar + <user>.session). Wir packen dieses Verzeichnis als JSON in
das Secret `photonestudios/icloud`. Das Apple-Passwort wird NIE gespeichert:
in der Cloud wird nur die vertrauenswürdige Sitzung (2FA bestätigt) wiederverwendet.
Ist sie abgelaufen, wirft open_service() LoginNeeded → Dani startet login.py neu.
"""
import json
import os

SECRET_ID = os.environ.get("PHOTON_ICLOUD_SECRET", "photonestudios/icloud")


class LoginNeeded(Exception):
    """Sitzung fehlt/abgelaufen — einmal `python login.py` auf dem Mac."""


def pack_dir(path):
    files = {}
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)
        if os.path.isfile(full):
            with open(full, encoding="utf-8") as f:
                files[name] = f.read()
    return files


def unpack_dir(files, path):
    os.makedirs(path, exist_ok=True)
    for name, content in files.items():
        if "/" in name or name.startswith("."):
            raise ValueError(f"ungültiger Dateiname im Secret: {name!r}")
        with open(os.path.join(path, name), "w", encoding="utf-8") as f:
            f.write(content)


def load(sm, path):
    """Secret holen, Dateien nach `path` schreiben. Liefert apple_id."""
    try:
        raw = sm.get_secret_value(SecretId=SECRET_ID)["SecretString"]
    except sm.exceptions.ResourceNotFoundException as e:
        raise LoginNeeded("Secret fehlt") from e
    data = json.loads(raw)
    unpack_dir(data["files"], path)
    return data["apple_id"]


def save(sm, apple_id, path, saved_at):
    body = json.dumps({"apple_id": apple_id, "saved_at": saved_at, "files": pack_dir(path)})
    try:
        sm.put_secret_value(SecretId=SECRET_ID, SecretString=body)
    except sm.exceptions.ResourceNotFoundException:
        sm.create_secret(Name=SECRET_ID, SecretString=body,
                         Description="iCloud-Sitzung (Cookies) für photon-icloud, staubsauger#2",
                         Tags=[{"Key": "project", "Value": "photon"}])


def open_service(apple_id, path, service_cls=None):
    """Vorhandene, vertrauenswürdige Sitzung wiederverwenden — nie mit Passwort.
    Nutzt pyicloud-Interna (_try_reuse_cached_session), Version ist im build.sh fest gepinnt."""
    if service_cls is None:
        from pyicloud import PyiCloudService as service_cls
    svc = service_cls(apple_id, password=None, cookie_directory=path, authenticate=False)
    if not svc._try_reuse_cached_session(False, False):
        raise LoginNeeded("Sitzung abgelaufen oder nicht vertrauenswürdig")
    svc._update_state()
    return svc
