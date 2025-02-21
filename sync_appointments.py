import os
import requests
import json
from urllib.parse import quote_plus


# Umgebungsvariablen aus GitHub Actions
ACCESS_TOKEN = os.getenv("BULLHORN_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("BULLHORN_REFRESH_TOKEN")
BHREST_TOKEN = os.getenv("BULLHORN_BHREST_TOKEN")
REST_URL = os.getenv("BULLHORN_REST_URL")  # https://rest70.bullhornstaffing.com/rest-services/7o3wld/
PLECTO_AUTH = os.getenv("PLECTO_AUTH")

# Unterschiedliche Swimlanes für OAuth und REST-API
OAUTH_SWIMLANE = "ger"
REST_SWIMLANE = "rest70"
CORP_TOKEN = "7o3wld"

def refresh_access_token(refresh_token):
    """ Holt einen neuen Access Token mit dem Refresh Token. """
    login_url = f"https://auth-{OAUTH_SWIMLANE}.bullhornstaffing.com/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": os.getenv("BULLHORN_CLIENT_ID"),
        "client_secret": os.getenv("BULLHORN_CLIENT_SECRET")
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(login_url, data=data, headers=headers)
    response.raise_for_status()
    tokens = response.json()
    print(f"🔄 Neuer Access Token: {tokens.get('access_token')}")
    return tokens.get("access_token"), tokens.get("refresh_token")

def get_bhrest_token(access_token):
    """ Holt BhRestToken und restUrl basierend auf dem Access Token. """
    login_url = (
        f"https://{REST_SWIMLANE}.bullhornstaffing.com/rest-services/{CORP_TOKEN}/login?version=*"
        f"&access_token={quote_plus(access_token)}"
    )
    print(f"🌐 Generierte Login-URL: {login_url}")  # Debug-Ausgabe
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(login_url, headers=headers)
    print(f"⚡ Response Status: {response.status_code} - {response.text}")  # Debug-Ausgabe
    response.raise_for_status()
    login_info = response.json()
    print("🗝️  BhRestToken:", login_info.get("BhRestToken"))
    print("🌐 REST URL:", login_info.get("restUrl"))
    return login_info.get("BhRestToken"), login_info.get("restUrl")

def get_appointments(bhrest_token, rest_url):
    """ Holt die Appointment-Daten aus Bullhorn. """
    rest_url = rest_url if rest_url.endswith('/') else rest_url + '/'
    endpoint = f"{rest_url}entity/Appointment?fields=id,owner,dateAdded,dateBegin&BhRestToken={bhrest_token}"
    print(f"🌐 Endpoint-URL: {endpoint}")  # Debug-Ausgabe
    response = requests.get(endpoint)
    print(f"⚡ Response Status: {response.status_code} - {response.text}")  # Debug-Ausgabe
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
