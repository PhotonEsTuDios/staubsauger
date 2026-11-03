#!/usr/bin/env python3
"""Einmaliger iCloud-Login auf dem Mac (staubsauger#2). Dani tippt alles selbst.

  AWS_PROFILE=photon-new ~/PhotonEsTuDios/.venv/bin/python icloud_cloud/login.py

Fragt Apple-ID + Passwort (unsichtbar) + 2FA-Code vom iPhone, macht die Sitzung
vertrauenswürdig und legt NUR die Sitzungs-Cookies im Secrets Manager ab
(`photonestudios/icloud`). Das Passwort verlässt den Mac nicht. Alle ~2 Monate wiederholen.
"""
import datetime as dt
import getpass
import os
import shutil
import sys
import tempfile

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_store  # noqa: E402


def main():
    from pyicloud import PyiCloudService
    apple_id = input("Apple-ID (E-Mail): ").strip()
    password = getpass.getpass("Apple-Passwort (wird nicht angezeigt, nicht gespeichert): ")
    tmp = tempfile.mkdtemp(prefix="photon-icloud-")
    try:
        api = PyiCloudService(apple_id, password, cookie_directory=tmp)
        del password
        if api.requires_2fa:
            code = input("2FA-Code vom iPhone: ").strip()
            if not api.validate_2fa_code(code):
                sys.exit("✗ Code falsch — bitte nochmal starten")
            if not api.is_trusted_session and not api.trust_session():
                sys.exit("✗ Sitzung nicht vertrauenswürdig — bitte nochmal starten")
        elif api.requires_2sa:
            sys.exit("✗ Altes 2-Schritt-Verfahren — bitte 2FA in der Apple-ID aktivieren")
        n = len(api.photos.all)
        print(f"✓ Angemeldet. Mediathek: {n} Einträge")
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        session_store.save(boto3.client("secretsmanager", region_name="eu-west-1"), apple_id, tmp, stamp)
        print(f"✓ Sitzung gespeichert in Secrets Manager {session_store.SECRET_ID} ({stamp})")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
