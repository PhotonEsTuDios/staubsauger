"""
PhotonEsTuDios — Refresh-Token holen UND direkt in AWS Secrets Manager schreiben.

Der Refresh-Token geht von Dropbox DIREKT ins Secret `photonestudios/dropbox`
und wird NIE ausgegeben (nicht im Chat, nicht im Terminal).

Zwei Modi:
  1) URL anzeigen (zum Autorisieren im Browser):
       python setup_secret.py --app-key APPKEY --auth-url
  2) Code einloesen + Secret schreiben (App-Secret & Code aus Dateien):
       AWS_PROFILE=photon python setup_secret.py \
         --app-key APPKEY --secret-file ~/Desktop/appsecret --code-file ~/Desktop/dropcode

App-Secret und Auth-Code werden aus Dateien gelesen (via `pbpaste > datei`),
damit nichts ueber die Kommandozeile/den Chat sichtbar wird.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

SECRET_NAME = "photonestudios/dropbox"
REGION = "eu-west-1"


def read_file(path):
    with open(os.path.expanduser(path)) as f:
        return f.read().strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app-key", required=True)
    ap.add_argument("--auth-url", action="store_true", help="nur Autorisierungs-URL ausgeben")
    ap.add_argument("--secret-file")
    ap.add_argument("--code-file")
    args = ap.parse_args()

    if args.auth_url:
        url = "https://www.dropbox.com/oauth2/authorize?" + urllib.parse.urlencode({
            "client_id": args.app_key,
            "response_type": "code",
            "token_access_type": "offline",
        })
        print("\nDiese URL im Browser oeffnen, 'Allow' klicken, den Code kopieren:\n")
        print("  " + url + "\n")
        return

    if not (args.secret_file and args.code_file):
        sys.exit("Brauche --secret-file und --code-file (oder --auth-url).")

    app_secret = read_file(args.secret_file)
    code = read_file(args.code_file)

    # Auth-Code -> Tokens (HTTP Basic app_key:app_secret)
    import base64
    data = urllib.parse.urlencode({
        "code": code,
        "grant_type": "authorization_code",
    }).encode()
    req = urllib.request.Request("https://api.dropboxapi.com/oauth2/token", data=data)
    basic = base64.b64encode(f"{args.app_key}:{app_secret}".encode()).decode()
    req.add_header("Authorization", "Basic " + basic)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit("Token-Tausch fehlgeschlagen: " + e.read().decode(errors="replace"))

    refresh = resp.get("refresh_token")
    if not refresh:
        sys.exit("Kein refresh_token erhalten (token_access_type=offline gesetzt? Code frisch?).")

    secret_json = json.dumps({
        "app_key": args.app_key,
        "app_secret": app_secret,
        "refresh_token": refresh,
    })

    import boto3
    sm = boto3.client("secretsmanager", region_name=REGION)
    try:
        sm.create_secret(Name=SECRET_NAME, SecretString=secret_json,
                         Description="Dropbox OAuth fuer photon-dropbox-ingest")
        action = "angelegt"
    except sm.exceptions.ResourceExistsException:
        sm.put_secret_value(SecretId=SECRET_NAME, SecretString=secret_json)
        action = "aktualisiert"

    print(f"\nOK — Secret '{SECRET_NAME}' {action}. (refresh_token nirgends ausgegeben)")
    print("App-Secret/Code-Dateien kannst du jetzt loeschen.")


if __name__ == "__main__":
    main()
