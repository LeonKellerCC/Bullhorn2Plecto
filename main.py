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
MANUAL_FALLBACK_REFRESH_TOKEN = "21535_8083665_70:a859fa15-b40b-4ab4-a4a1-3cd555ca962e"

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
DATA_SOURCE_UUID = "4a95b33cba6a44e49eaf44011fc3d448"  # Beispiel: "70a0d1kg780a4cd98f541c214601030e"

# ===============================
# Funktionen
# ===============================
def get_bhresttoken_and_resturl(access_token):
    """
    Holt BhRestToken und REST-URL von Bullhorn anhand des Access Tokens.
    """
    login_url = f"https://rest-{OAUTH_SWIMLANE}.bullhornstaffing.com/rest-services/login?version=2.0&access_token={access_token}"
    print(f"🌐 Abrufen von BhRestToken und REST URL von: {login_url}")
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
    Das Payload enthält einen Titel und eine Liste von Feldern.
    """
    url = "https://app.plecto.com/api/v2/datasources/"
    auth = (plecto_email, plecto_password)
    headers = {"Content-Type": "application/json"}
    payload = {
        "title": "My Bullhorn Meeting Notes",
        "fields": [
            {
                "name": "note_id",
                "input": "TextInput",
                "default_value": ""
            },
            {
                "name": "date_added",
                "input": "TextInput",  # Wir verwenden hier den Timestamp aus dateAdded
                "default_value": ""
            },
            {
                "name": "action",
                "input": "TextInput",
                "default_value": ""
            },
            {
                "name": "commenting_person",
                "input": "TextInput",
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


def get_meeting_notes(bhrest_token, rest_url, desired_action="Meeting"):
    """
    Ruft alle Notes von Bullhorn über den Search-Endpunkt ab und filtert lokal:
      - Es werden nur Notes mit dem gewünschten action-Wert berücksichtigt.
      - Es werden nur Notes verarbeitet, die innerhalb der letzten zwei Wochen liegen.
    
    Der Search-Endpunkt benötigt einen "query"-Parameter; wir verwenden hier den Wildcard-Query "*"
    und fordern zusätzlich eine Sortierung absteigend nach dateAdded an.
    """
    if not rest_url.endswith("/"):
        rest_url += "/"
    all_notes = []
    start = 0
    count = 100  # Anzahl der Datensätze pro Anfrage
    endpoint = f"{rest_url}search/Note"

    # Berechne den Schwellenwert (zwei Wochen zurück, in Millisekunden)
    now = datetime.datetime.now(datetime.timezone.utc)
    threshold_dt = now - datetime.timedelta(weeks=2)
    threshold_ms = int(threshold_dt.timestamp() * 1000)
    print(f"Schwellenwert (dateAdded, letzte 2 Wochen): {threshold_ms}")

    while True:
        params = {
            "query": "*",  # Wildcard: Alle Notes abrufen
            "fields": "id,action,dateAdded,commentingPerson(id,firstName,lastName)",
            "sort": "-dateAdded",  # Neueste zuerst
            "start": start,
            "count": count,
            "BhRestToken": bhrest_token
        }
        print(f"📅 Abrufe Notes (Start: {start})")
        headers = {"Accept": "application/json"}
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Fehler beim Abrufen der Notes: {response.status_code}")
            print(response.text)
            break
        data = response.json()
        notes = data.get("data", [])
        if not notes:
            print("✅ Keine weiteren Notes gefunden.")
            break

        # Debug: Ausgabe der in diesem Batch gefundenen action-Werte
        actions_in_batch = {note.get("action") for note in notes}
        print(f"DEBUG: Gefundene action-Werte in diesem Batch: {actions_in_batch}")

        # Da die Ergebnisse absteigend sortiert sind,
        # können wir die Schleife abbrechen, wenn ein Note älter als der Schwellenwert ist.
        stop_loop = False
        for note in notes:
            note_date = note.get("dateAdded", 0)
            if note_date < threshold_ms:
                stop_loop = True
                break  # Alle folgenden Einträge sind dann auch älter
            if note.get("action") == desired_action:
                all_notes.append(note)
        if stop_loop:
            print("🔚 Ältere Einträge erreicht – beende Abruf-Schleife.")
            break
        start += count
    print(f"✅ Insgesamt {len(all_notes)} Notes mit action='{desired_action}' aus den letzten 2 Wochen abgerufen.")
    return {"data": all_notes}


def send_registrations_to_plecto(notes_dict, data_source_uuid, plecto_email, plecto_password):
    """
    Transformiert die Bullhorn-Meeting-Note-Daten in Registrierungen für Plecto und
    sendet diese als Bulk-Request an den Endpoint: https://app.plecto.com/api/v2/registrations/
    Die Daten werden in Batches von je 100 Einträgen gesendet.
    """
    notes = notes_dict.get("data", [])
    registrations = []
    url = "https://app.plecto.com/api/v2/registrations/"
    auth = (plecto_email, plecto_password)
    headers = {"Content-Type": "application/json"}
    
    for note in notes:
        commenter = note.get("commentingPerson", {})
        date_added_ms = note.get("dateAdded")
        date_added_iso = datetime.datetime.fromtimestamp(date_added_ms / 1000, datetime.timezone.utc).isoformat() if date_added_ms else None

        registration = {
            "data_source": data_source_uuid,
            "member_api_provider": "Bullhorn",
            "member_api_id": str(commenter.get("id")),
            "member_name": f"{commenter.get('firstName', '')} {commenter.get('lastName', '')}".strip(),
            "external_id": str(note.get("id")),
            "note_id": str(note.get("id")),
            "date_added": date_added_iso,
            "action": note.get("action")
        }
        registrations.append(registration)
        
        if len(registrations) == 100:
            print("📤 Sende 100 Registrierungen an Plecto...")
            response = requests.post(url, auth=auth, headers=headers, data=json.dumps(registrations))
            response.raise_for_status()
            print("✅ 100 Registrierungen erfolgreich gesendet.")
            registrations = []
    
    if registrations:
        print(f"📤 Sende letzte {len(registrations)} Registrierungen an Plecto...")
        response = requests.post(url, auth=auth, headers=headers, data=json.dumps(registrations))
        response.raise_for_status()
        print("✅ Letzte Registrierungen erfolgreich gesendet.")


def main():
    global DATA_SOURCE_UUID
    if DATA_SOURCE_UUID is None:
        DATA_SOURCE_UUID = create_plecto_datasource(PLECTO_EMAIL, PLECTO_PASSWORD)
        if DATA_SOURCE_UUID is None:
            DATA_SOURCE_UUID = "4a95b33cba6a44e49eaf44011fc3d448"
    
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
    notes = get_meeting_notes(bhrest_token, rest_url, desired_action="Meeting")
    send_registrations_to_plecto(notes, DATA_SOURCE_UUID, PLECTO_EMAIL, PLECTO_PASSWORD)


if __name__ == "__main__":
    try:
        main()
        print("🎉 Prozess abgeschlossen. Bullhorn-Meeting-Notes wurden abgerufen, die Data Source erstellt und Registrierungen an Plecto gesendet.")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTPError: {str(http_err)}")
    except Exception as e:
        print(f"❌ Fehler: {e}")
