"""
PhotonEsTuDios — Dropbox Refresh-Token Helfer (einmalig, lokal).

Erzeugt aus App-Key/-Secret einen langlebigen Refresh-Token für die
Cloud-Lambda. Laeuft NUR lokal; gibt den Token im Terminal aus (NICHT im Chat,
NICHT ins Git). App-Secret wird verdeckt eingegeben (kein Echo).

Ablauf:
  python3 get_refresh_token.py
  -> App-Key/-Secret eingeben
  -> angezeigte URL im Browser oeffnen, "Allow" klicken, Code kopieren
  -> Code einfuegen
  -> refresh_token wird ausgegeben

Die drei Werte (app_key, app_secret, refresh_token) gehen dann an Rainer
fuer den Secrets-Manager-Eintrag `photonestudios/dropbox` -- per sicherem
Kanal, nicht im Chat.
"""
import getpass
import json
import urllib.parse
import urllib.request


def main():
    print("\n=== PhotonEsTuDios — Dropbox Refresh-Token ===\n")
    app_key = input("App key:    ").strip()
    app_secret = getpass.getpass("App secret (versteckt): ").strip()

    # 1) Authorize-URL bauen (offline => Refresh-Token, ohne redirect_uri => Code am Bildschirm)
    auth_url = "https://www.dropbox.com/oauth2/authorize?" + urllib.parse.urlencode({
        "client_id": app_key,
        "response_type": "code",
        "token_access_type": "offline",
    })
    print("\n1) Diese URL im Browser oeffnen und 'Allow' klicken:\n")
    print("   " + auth_url + "\n")
    print("2) Dropbox zeigt dann einen Authorization-Code an — kopieren.\n")
    code = input("3) Code hier einfuegen: ").strip()

    # 4) Code gegen Tokens tauschen (HTTP Basic Auth app_key:app_secret)
    data = urllib.parse.urlencode({
        "code": code,
        "grant_type": "authorization_code",
    }).encode()
    req = urllib.request.Request("https://api.dropboxapi.com/oauth2/token", data=data)
    basic = base64_basic(app_key, app_secret)
    req.add_header("Authorization", "Basic " + basic)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.load(r)
    except urllib.error.HTTPError as e:
        print("\nFEHLER beim Token-Tausch:", e.read().decode(errors="replace"))
        return

    refresh = resp.get("refresh_token")
    if not refresh:
        print("\nKein refresh_token erhalten. Antwort:", resp)
        print("Tipp: token_access_type=offline noetig, und App-Permissions vorher gesetzt?")
        return

    print("\n=== FERTIG — diese Werte sicher an Rainer (NICHT in den Chat) ===\n")
    print(json.dumps({
        "app_key": app_key,
        "app_secret": app_secret,
        "refresh_token": refresh,
    }, indent=2))
    print("\nDas ist exakt das JSON fuer Secrets Manager `photonestudios/dropbox`.\n")


def base64_basic(user, pw):
    import base64
    return base64.b64encode(f"{user}:{pw}".encode()).decode()


if __name__ == "__main__":
    main()
