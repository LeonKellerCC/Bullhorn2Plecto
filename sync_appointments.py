import os
import requests
import json

# Umgebungsvariablen aus GitHub Actions
ACCESS_TOKEN = os.getenv("BULLHORN_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("BULLHORN_REFRESH_TOKEN")
BHREST_TOKEN = os.getenv("BULLHORN_BHREST_TOKEN")
REST_URL = os.getenv("BULLHORN_REST_URL")
PLECTO_AUTH = os.getenv("PLECTO_AUTH")
SWIMLANE = "ger"

def refresh_access_token(refresh_token):
    """ Holt einen neuen Access Token, wenn der alte abgelaufen ist. """
    url = f"https://auth-{SWIMLANE}.bullhornstaffing.com/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": os.getenv("BULLHORN_CLIENT_ID"),
        "client_secret": os.getenv("BULLHORN_CLIENT_SECRET")
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    tokens = response.json()
    print(f"🔄 Neuer Access Token: {tokens.get('access_token')}")
    return tokens

def get_appointments():
    """ Holt die Appointment-Daten aus Bullhorn. """
    rest_url = REST_URL if REST_URL.endswith('/') else REST_URL + '/'

    endpoint = f"{rest_url}entity/Appointment?fields=id,owner,dateAdded,dateBegin&BhRestToken={BHREST_TOKEN}"
    print(f"🌐 Endpoint-URL: {endpoint}")  # Debug-Ausgabe zur Überprüfung
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
    appointments = get_appointments()
    if appointments:
        send_to_plecto(appointments)
    else:
        print("ℹ️ Keine neuen Appointments gefunden.")
