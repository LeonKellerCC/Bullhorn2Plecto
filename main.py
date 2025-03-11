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
# ===============================
PLECTO_EMAIL = os.environ.get("PLECTO_EMAIL")
PLECTO_PASSWORD = os.environ.get("PLECTO_PASSWORD")

# Falls Du schon eine Data Source in Plecto erstellt hast,
# kannst Du hier deren UUID eintragen. Ansonsten wird versucht,
# eine neue Data Source anzulegen.
DATA_SOURCE_UUID = "4a95b33cba6a44e49eaf44011fc3d448"

# ===============================
# Funktionen
# ===============================
def get_bhresttoken_and_resturl(access_token):
    """
    Holt BhRestToken und Rest-URL von Bullhorn anhand des Access Tokens.
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
    Die POST-Anfrage wird an https://app.plecto.com/api/v2/datasources/ gesendet.
    
    Das Payload enthält einen Titel und eine Liste von Feldern.
    """
    url = "https://app.plecto.com/api/v2/datasources/"
    auth = (plecto_email, plecto_password)
    headers = {"Content-Type": "application/json"}
    payload = {
        "title": "My Bullhorn Meetings",
        "fields": [
            {
                "name": "note_id",
                "input": "TextInput",
                "default_value": ""
            },
            {
                "name": "date_added",
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


def get_meeting_notes(bhrest_token, rest_url):
    """
    Ruft alle Notes ab, bei denen action='Meeting' gesetzt ist (mit Pagination).
    """
    if not rest_url.endswith("/"):
        rest_url += "/"
    all_notes = []
    start = 0
    count = 100
    where_clause = "action='Meeting'"
    
    while True:
        endpoint = (
            f"{rest_url}query/Note"
            f"?BhRestToken={bhrest_token}"
            f"&fields=id,owner(id,firstName,lastName),dateAdded"  # <-- 'comments' entfernt
            f"&where={where_clause}&start={start}&count={count}"
        )
        print(f"📅 Abrufe Meeting-Notes (Start: {start})")
        headers = {"Accept": "application/json"}
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            print(f"❌ Fehler beim Abrufen der Notes: {response.status_code}")
            print(response.text)
            break
        
        data = response.json()
        notes = data.get("data", [])
        if not notes:
            print("✅ Keine weiteren Meeting-Notes gefunden.")
            break
        
        all_notes.extend(notes)
        start += count
    
    print(f"✅ Insgesamt {len(all_notes)} Meeting-Notes abgerufen.")
    
    # Speichern als Debug-Datei (optional)
    with open("debug_meeting_notes.json", "w", encoding="utf-8") as f:
        json.dump({"data": all_notes}, f, indent=4)
    
    return {"data": all_notes}


def send_meeting_notes_to_plecto(notes_dict, data_source_uuid, plecto_email, plecto_password):
    """
    Transformiert die Meeting-Notes in Registrierungen für Plecto
    und sendet diese als Bulk-Request an: https://app.plecto.com/api/v2/registrations/
    """
    notes = notes_dict.get("data", [])
    registrations = []
    url = "https://app.plecto.com/api/v2/registrations/"
    auth = (plecto_email, plecto_password)
    headers = {"Content-Type": "application/json"}
    
    for note in notes:
        owner = note.get("owner", {})
        date_added_ms = note.get("dateAdded")
        date_added_iso = None
        
        if date_added_ms:
            date_added_iso = datetime.datetime.fromtimestamp(date_added_ms / 1000, datetime.timezone.utc).isoformat()
        
        registration = {
            "data_source": data_source_uuid,
            "member_api_provider": "Bullhorn",
            "member_api_id": str(owner.get("id")),
            "member_name": f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip(),
            "external_id": str(note.get("id")),
            "note_id": str(note.get("id")),
            "date_added": date_added_iso
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
    
    notes_dict = get_meeting_notes(bhrest_token, rest_url)
    
    send_meeting_notes_to_plecto(notes_dict, DATA_SOURCE_UUID, PLECTO_EMAIL, PLECTO_PASSWORD)


if __name__ == "__main__":
    try:
        main()
        print("🎉 Prozess abgeschlossen. Meeting-Notes wurden abgerufen, die Data Source erstellt und Registrierungen an Plecto gesendet.")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTPError: {str(http_err)}")
    except Exception as e:
        print(f"❌ Fehler: {e}")
