import requests
import os
import json
import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# ===============================
# Bullhorn-Konfiguration
# ===============================
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REDIRECT_URI = "https://welcome.bullhornstaffing.com"
OAUTH_SWIMLANE = "ger"
MANUAL_FALLBACK_REFRESH_TOKEN = "21535_8083665_70:80f09780-16fe-49df-9a48-3b47a98b7525"

# ===============================
# Azure Key Vault (zur Token-Verwaltung)
# ===============================
KV_URL = os.environ.get("KEY_VAULT_URL")
if not KV_URL:
    print("⚠️  KEY_VAULT_URL nicht gesetzt. Bitte setze die Umgebungsvariable!")
AZURE_CREDENTIALS = os.environ.get("AZURE_CREDENTIALS")
if not AZURE_CREDENTIALS:
    print("⚠️  AZURE_CREDENTIALS nicht gefunden. Stelle sicher, dass sie in den Umgebungsvariablen gesetzt sind.")

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KV_URL, credential=credential)

# ===============================
# Plecto-Konfiguration (Basic Auth)
# Erstelle in Plecto einen dedizierten Benutzer für die Integration!
# ===============================
PLECTO_EMAIL = os.environ.get("PLECTO_EMAIL")
PLECTO_PASSWORD = os.environ.get("PLECTO_PASSWORD")

# Falls Du schon eine Data Source in Plecto erstellt hast,
# kannst Du hier deren UUID eintragen. Ansonsten wird versucht,
# eine neue Data Source anzulegen.
DATA_SOURCE_UUID = "4a95b33cba6a44e49eaf44011fc3d448"  # z. B. "70a0d1kg780a4cd98f541c214601030e"


# ===============================
# Funktionen
# ===============================
def get_bhresttoken_and_resturl(access_token):
    """
    Holt BhRestToken und Rest-URL von Bullhorn anhand des Access Tokens.
    """
    login_url = f"https://rest-{OAUTH_SWIMLANE}.bullhornstaffing.com/rest-services/login?version=2.0&access_token={access_token}"
    print(f"🌐 Abrufen von BhRestToken und Rest-URL von: {login_url}")
    response = requests.post(login_url)
    response.raise_for_status()
    login_info = response.json()
    bhrest_token = login_info.get("BhRestToken")
    rest_url = login_info.get("restUrl")
    print("✅ BhRestToken:", bhrest_token)
    print("🌐 REST URL:", rest_url)
    return bhrest_token, rest_url


def create_plecto_datasource(plecto_email, plecto_password):
    """
    Erstellt eine neue API Data Source in Plecto über Basic Authentication.
    Die POST-Anfrage wird an https://app.plecto.com/api/v2/datasources/ gesendet.
    
    Das Payload enthält einen Titel und eine Liste von Feldern.
    """
    url = "https://app.plecto.com/api/v2/datasources/"
    auth = (plecto_email, plecto_password)
    headers = {"Content-Type": "application/json"}
    payload = {
        "title": "My Bullhorn Appointments",
        "fields": [
            {
                "name": "appointment_id",
                "input": "TextInput",
                "default_value": ""
            },
            {
                "name": "date_added",
                "input": "TextInput",  # Statt DateTimeInput
                "default_value": ""
            },
            {
                "name": "date_begin",
                "input": "TextInput",  # Statt DateTimeInput
                "default_value": ""
            }
        ]
    }
    print("📤 Erstelle Plecto Data Source...")
    response = requests.post(url, auth=auth, headers=headers, data=json.dumps(payload))
    if response.status_code == 201:
        ds = response.json()
        print("✅ Data Source erfolgreich erstellt!")
        print("Antwort:", ds)
        return ds.get("id")
    else:
        print("❌ Fehler beim Erstellen der Data Source:")
        print(response.status_code, response.text)
        return None


def get_appointments(bhrest_token, rest_url):
    """
    Ruft alle Appointment-Daten von Bullhorn über den Query-Endpunkt ab.
    """
    if not rest_url.endswith("/"):
        rest_url += "/"
    where_clause = "id>0"
    start = 0
    count = 100  # Anzahl der Datensätze pro Anfrage (ggf. paginieren, wenn mehr benötigt werden)
    endpoint = (f"{rest_url}query/Appointment?BhRestToken={bhrest_token}"
                f"&fields=id,owner,dateAdded,dateBegin"
                f"&where={where_clause}&start={start}&count={count}")
    print(f"📅 Abfrage-Endpunkt-URL: {endpoint}")
    headers = {"Accept": "application/json"}
    response = requests.get(endpoint, headers=headers)
    print("===== DEBUG: APPOINTMENT RESPONSE =====")
    print("Status Code =", response.status_code)
    print("Response Text =", response.text)
    print("=======================================\n")
    response.raise_for_status()
    appointments = response.json()
    print("✅ Abgerufene Appointment-Daten:", appointments)
    return appointments


def send_registrations_to_plecto(appointments, data_source_uuid, plecto_email, plecto_password):
    """
    Transformiert die Bullhorn-Appointment-Daten in Registrierungen für Plecto und
    sendet diese als Bulk-Request an den Endpoint:
    https://app.plecto.com/api/v2/registrations/
    
    Für jede Registrierung sind folgende Pflichtfelder enthalten:
      - data_source: Die UUID der Plecto Data Source
      - member_api_provider: Hier "Bullhorn" (als Beispiel)
      - member_api_id: Die Owner-ID (als String)
      - member_name: Zusammengesetzt aus firstName und lastName
      - external_id: Hier die Appointment-ID (als String)
      
    Zusätzlich werden die Felder "date_added" und "date_begin" im ISO8601-Format übermittelt.
    """
    registrations = []
    for appointment in appointments.get("data", []):
        owner = appointment.get("owner", {})
        # Konvertiere die Zeitstempel (in ms) in ISO8601 mit Zeitzonenoffset
        date_added_ms = appointment.get("dateAdded")
        date_begin_ms = appointment.get("dateBegin")
        if date_added_ms:
            date_added_iso = datetime.datetime.fromtimestamp(date_added_ms / 1000, datetime.timezone.utc).isoformat()
        else:
            date_added_iso = None
        if date_begin_ms:
            date_begin_iso = datetime.datetime.fromtimestamp(date_begin_ms / 1000, datetime.timezone.utc).isoformat()
        else:
            date_begin_iso = None

        registration = {
            "data_source": data_source_uuid,
            "member_api_provider": "Bullhorn",
            "member_api_id": str(owner.get("id")),
            "member_name": f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip(),
            "external_id": str(appointment.get("id")),
            # Optionale Felder, die in der Data Source definiert sind:
            "appointment_id": str(appointment.get("id")),
            "date_added": date_added_iso,
            "date_begin": date_begin_iso
        }
        registrations.append(registration)
    
    # Bulk-Registrierungen (bis zu 100 pro Request)
    url = "https://app.plecto.com/api/v2/registrations/"
    auth = (plecto_email, plecto_password)
    headers = {"Content-Type": "application/json"}
    print("📤 Sende Registrierungen an Plecto...")
    response = requests.post(url, auth=auth, headers=headers, data=json.dumps(registrations))
    response.raise_for_status()
    print("✅ Registrierungen erfolgreich an Plecto gesendet!")
    print("Plecto Antwort:", response.text)


def main():
    # Optional: Erstelle in Plecto eine neue Data Source, falls noch nicht vorhanden.
    global DATA_SOURCE_UUID
    if DATA_SOURCE_UUID is None:
        DATA_SOURCE_UUID = create_plecto_datasource(PLECTO_EMAIL, PLECTO_PASSWORD)
        if DATA_SOURCE_UUID is None:
            # Falls das Erstellen fehlschlägt (z. B. weil sie schon existiert),
            # kannst Du hier alternativ die vorhandene UUID eintragen.
            DATA_SOURCE_UUID = "4a95b33cba6a44e49eaf44011fc3d448"
    
    # Bullhorn: Refresh Token abrufen (aus dem Key Vault oder Umgebungsvariablen)
    try:
        refresh_token_secret = secret_client.get_secret("BullhornRefreshToken")
        REFRESH_TOKEN = refresh_token_secret.value
        print("🔑 Refresh Token erfolgreich aus dem Key Vault geladen.")
    except Exception as ex:
        REFRESH_TOKEN = os.getenv("BULLHORN_INITIAL_REFRESH_TOKEN")
        print("⚠️  Kein gespeichertes Refresh Token gefunden. Verwende initiales Token.")
    
    if not REFRESH_TOKEN or REFRESH_TOKEN.strip() == "":
        print("⚠️  Refresh Token ungültig oder nicht vorhanden. Verwende manuelles Fallback-Token.")
        REFRESH_TOKEN = MANUAL_FALLBACK_REFRESH_TOKEN
    
    print("===== DEBUG: DIRECT CREDENTIALS =====")
    print("CLIENT_ID =", CLIENT_ID)
    print("CLIENT_SECRET =", CLIENT_SECRET)
    print("REFRESH_TOKEN =", REFRESH_TOKEN)
    print("REDIRECT_URI =", REDIRECT_URI)
    print("OAUTH_SWIMLANE =", OAUTH_SWIMLANE)
    print("======================================\n")
    
    # Bullhorn: POST-Anfrage zum Abrufen des Access Tokens
    token_url = f"https://auth-{OAUTH_SWIMLANE}.bullhornstaffing.com/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI
    }
    headers_token = {"Content-Type": "application/x-www-form-urlencoded"}
    
    print("===== DEBUG: REFRESH REQUEST =====")
    print("URL =", token_url)
    print("PAYLOAD =", data)
    print("HEADERS =", headers_token)
    print("==================================\n")
    
    response = requests.post(token_url, data=data, headers=headers_token)
    
    if response.status_code == 400 and "invalid_grant" in response.text:
        print("⚠️  Refresh Token ungültig oder abgelaufen! Verwende Fallback-Token.")
        REFRESH_TOKEN = MANUAL_FALLBACK_REFRESH_TOKEN
        data["refresh_token"] = REFRESH_TOKEN
        response = requests.post(token_url, data=data, headers=headers_token)
    
    print("===== DEBUG: RESPONSE =====")
    print("Status Code =", response.status_code)
    print("Response Text =", response.text)
    print("===========================\n")
    
    response.raise_for_status()
    tokens = response.json()
    new_access_token = tokens.get("access_token")
    new_refresh_token = tokens.get("refresh_token")
    
    print("✅ Neuer Access Token:", new_access_token)
    print("🔄 Neuer Refresh Token:", new_refresh_token)
    print("📅 expires_in:", tokens.get("expires_in"))
    
    secret_client.set_secret("BullhornRefreshToken", new_refresh_token)
    print("💾 Refresh Token erfolgreich im Key Vault gespeichert.")
    
    bhrest_token, rest_url = get_bhresttoken_and_resturl(new_access_token)
    appointments = get_appointments(bhrest_token, rest_url)
    
    # Sende die Bullhorn-Daten als Registrierungen an Plecto
    send_registrations_to_plecto(appointments, DATA_SOURCE_UUID, PLECTO_EMAIL, PLECTO_PASSWORD)

if __name__ == "__main__":
    try:
        main()
        print("🎉 Prozess abgeschlossen. Bullhorn-Appointments wurden abgerufen, die Data Source erstellt und Registrierungen an Plecto gesendet.")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTPError: {str(http_err)}")
    except Exception as e:
        print(f"❌ Fehler: {e}")
