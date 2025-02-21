import os
import json
import requests
from urllib.parse import quote_plus

#
# sync_appointments.py
#
# Liest die Bullhorn- und Plecto-Credentials aus den Umgebungsvariablen (z.B. GitHub Secrets),
# führt den Refresh-Token-Flow durch, holt das BhRestToken und ruft anschließend
# die Appointment-Daten ab, um sie an Plecto zu senden.
#


# ===== 1. KONFIGURATION AUS UMGEBUNGSVARIABLEN =====
# Diese Variablen solltest du in GitHub Actions unter "Settings > Secrets and variables > Actions" hinterlegen
CLIENT_ID = os.getenv("BULLHORN_CLIENT_ID")
CLIENT_SECRET = os.getenv("BULLHORN_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("BULLHORN_REFRESH_TOKEN")
BULLHORN_REDIRECT_URI = os.getenv("BULLHORN_REDIRECT_URI", "https://welcome.bullhornstaffing.com")
OAUTH_SWIMLANE = os.getenv("OAUTH_SWIMLANE", "ger")       # "ger" oder "eu", je nach Setup
REST_SWIMLANE = os.getenv("REST_SWIMLANE", "rest70")      # "rest70", "rest-ger", etc.
PLECTO_AUTH = os.getenv("PLECTO_AUTH")                    # Basic Auth: "user:password"


# ===== 2. FUNKTIONEN FÜR BULLHORN-AUTHENTIFIZIERUNG & API-ZUGRIFF =====

def refresh_access_token(refresh_tkn: str) -> tuple[str, str]:
    """
    Holt einen neuen Access Token mithilfe des Refresh Tokens.
    Gibt (access_token, refresh_token) zurück.
    """
    token_url = f"https://auth-{OAUTH_SWIMLANE}.bullhornstaffing.com/oauth/token"

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_tkn,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": BULLHORN_REDIRECT_URI
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    print("=== DEBUG: Refresh-Token-Request ===")
    print("URL:", token_url)
    print("PAYLOAD:", payload)
    print("HEADERS:", headers)
    print("====================================\n")

    resp = requests.post(token_url, data=payload, headers=headers)
    print("Refresh-Flow Status:", resp.status_code)
    print("Refresh-Flow Response:", resp.text)

    resp.raise_for_status()  # Werfe Exception bei 4xx/5xx

    tokens = resp.json()
    new_access = tokens.get("access_token")
    new_refresh = tokens.get("refresh_token")
    expires_in = tokens.get("expires_in")

    print(f"✅ Neuer Access Token: {new_access}")
    print(f"🔄 Neuer Refresh Token: {new_refresh}")
    print(f"⌛ Gültig (Sekunden): {expires_in}\n")

    return new_access, new_refresh


def get_bhrest_token(access_tkn: str) -> tuple[str, str]:
    """
    Holt BhRestToken und restUrl via POST /login?access_token=...
    Wichtig: KEIN corpToken im Pfad.
    """
    login_url = (
        f"https://{REST_SWIMLANE}.bullhornstaffing.com/rest-services/login"
        f"?version=2.0&access_token={quote_plus(access_tkn)}"
    )
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    print("=== DEBUG: BhRestToken-Request ===")
    print("URL:", login_url)
    print("HEADERS:", headers)
    print("===============================\n")

    resp = requests.post(login_url, headers=headers)
    print("BhRestToken Status:", resp.status_code)
    print("BhRestToken Response:", resp.text)

    resp.raise_for_status()
    info = resp.json()

    bhrest_token = info.get("BhRestToken")
    rest_url = info.get("restUrl")

    print(f"🗝️  BhRestToken: {bhrest_token}")
    print(f"🌐 REST URL: {rest_url}\n")

    return bhrest_token, rest_url


def get_appointments(bhrest_tkn: str, url_rest: str) -> list[dict]:
    """
    Fragt Appointment-Daten ab und gibt sie als Liste von Dicts zurück.
    """
    if not url_rest.endswith("/"):
        url_rest += "/"

    endpoint = f"{url_rest}entity/Appointment"
    params = {
        "fields": "id,owner,dateAdded,dateBegin",
        "BhRestToken": bhrest_tkn
    }

    print("=== DEBUG: GET Appointments ===")
    print("URL:", endpoint)
    print("PARAMS:", params)
    print("==============================\n")

    resp = requests.get(endpoint, params=params)
    print("Appointments Status:", resp.status_code)
    print("Appointments Response:", resp.text, "\n")

    resp.raise_for_status()
    data = resp.json().get("data", [])
    print(f"📅 {len(data)} Appointments abgerufen.\n")

    return data


def send_to_plecto(appointments: list[dict]) -> None:
    """
    Sendet Appointment-Daten an Plecto via Basic Auth (user:password).
    """
    url = "https://app.plecto.com/api/v2/registrations/"
    headers = {
        "Authorization": f"Basic {PLECTO_AUTH}",
        "Content-Type": "application/json"
    }

    print("=== DEBUG: POST to Plecto ===")
    for appt in appointments:
        payload = {
            "data_source": "DEINE_DATASOURCE_UUID",  # Eigenen Wert einfügen
            "member_api_provider": "Bullhorn",
            "member_api_id": str(appt["owner"]["id"]),
            "member_name": f"Owner_{appt['owner']['id']}",
            "external_id": str(appt["id"]),
            "date": appt["dateBegin"]
        }

        r = requests.post(url, headers=headers, json=payload)
        print(f"🚀 Sende Appointment {appt['id']} an Plecto – Status: {r.status_code}")

        if r.status_code == 201:
            print(f"✅ Erfolgreich: {appt['id']}\n")
        else:
            print(f"⚠️ Fehler bei {appt['id']}: {r.text}\n")


# ===== 3. HAUPTABLAUF =====

def main() -> None:
    print("🚀 Starte Bullhorn → Plecto Sync...\n")

    # 1. Per Refresh Token neuen Access Token holen
    new_access, new_refresh = refresh_access_token(REFRESH_TOKEN)

    # 2. Mit Access Token BhRestToken & restUrl abrufen
    bhrest, rest_url = get_bhrest_token(new_access)

    # 3. Appointment-Daten abrufen
    appointments = get_appointments(bhrest, rest_url)
    if not appointments:
        print("ℹ️ Keine Appointments gefunden oder Liste leer.")
        return

    # 4. Daten an Plecto senden
    send_to_plecto(appointments)

    print("✅ Sync erfolgreich abgeschlossen.")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTPError: {http_err}")
    except Exception as e:
        print(f"❌ Fehler: {e}")
