import os
import requests
import json

# Umgebungsvariablen aus GitHub Actions
CLIENT_ID = os.getenv("BULLHORN_CLIENT_ID")
CLIENT_SECRET = os.getenv("BULLHORN_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("BULLHORN_REFRESH_TOKEN")
REST_URL = os.getenv("BULLHORN_REST_URL")  # https://rest70.bullhornstaffing.com/rest-services/7o3wld/
SWIMLANE = "ger"

PLECTO_AUTH = os.getenv("PLECTO_AUTH")

def refresh_access_token(refresh_token):
    """ Holt einen neuen Access Token mit dem Refresh Token. """
    url = f"https://auth70.bullhornstaffing.com/oauth/token"  # 🔧 Korrektur hier
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=payload, headers=headers)
    response.raise_for_status()
    tokens = response.json()
    print(f"🔄 Neuer Access Token erhalten.")
    return tokens.get("access_token"), tokens.get("refresh_token")

def get_bhrest_token(access_token):
    """ Holt BhRestToken und restUrl basierend auf dem Access Token. """
    login_url = (
        f"https://rest-{SWIMLANE}.bullhornstaffing.com/rest-services/login?version=2.0"
        f"&access_token={access_token}"
    )
    response = requests.post(login_url)
    response.raise_for_status()
    login_info = response.json()
    print("🗝️ BhRestToken abgerufen.")
    return login_info.get("BhRestToken"), login_info.get("restUrl")

def get_appointments(bhrest_token, rest_url):
    """ Holt die Appointment-Daten aus Bullhorn. """
    endpoint = f"{rest_url}entity/Appointment?fields=id,owner,dateAdded,dateBegin&BhRestToken={bhrest_token}"
    print(f"🌐 Endpoint-URL: {endpoint}")
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json().get("data", [])
    print(f"📅 {len(data)} Appointments abgerufen.")
    return data

def send_to_plecto(appointments):
    """ Sendet die Appointment-Daten an Plecto. """
    url = "https://app.plecto.com/api/v2/registrations/"
    headers = {
        "Authorization": f"Basic {PLECTO_AUTH}",
        "Content-Type": "application/json"
    }
    for appointment in appointments:
        payload = {
            "data_source": "DEINE_DATASOURCE_UUID",  # ❗ Hier deine Plecto DataSource UUID einfügen
            "member_api_provider": "Bullhorn",
            "member_api_id": str(appointment["owner"]["id"]),
            "member_name": f"Owner_{appointment['owner']['id']}",
            "external_id": str(appointment["id"]),
            "date": appointment["dateBegin"]
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            print(f"✅ Appointment {appointment['id']} erfolgreich an Plecto gesendet.")
        else:
            print(f"⚠️ Fehler bei Appointment {appointment['id']}: {response.text}")

if __name__ == "__main__":
    print("🚀 Starte Synchronisierung...")
    access_token, REFRESH_TOKEN = refresh_access_token(REFRESH_TOKEN)
    bhrest_token, rest_url = get_bhrest_token(access_token)
    appointments = get_appointments(bhrest_token, rest_url)

    if appointments:
        send_to_plecto(appointments)
    else:
        print("ℹ️ Keine neuen Appointments gefunden.")

    print("✅ Prozess abgeschlossen. Synchronisierung erfolgreich.")
