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

# Falls bereits eine Data Source in Plecto existiert, hier deren UUID eintragen.
DATA_SOURCE_UUID = "4a95b33cba6a44e49eaf44011fc3d448"


def get_bhresttoken_and_resturl(access_token):
    """
    Holt BhRestToken und REST URL von Bullhorn anhand des Access Tokens.
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


def debug_actions_table(bhrest_token, rest_url):
    """
    Ruft bis zu 500 Notizen (Notes) ab (beginnend mit den neuesten), indem in mehreren Requests jeweils 100 Notizen abgerufen werden.
    Dabei wird der Query-Parameter auf "id:*" gesetzt, um alle Notizen zu erfassen.
    Anschließend wird eine Übersicht der verschiedenen action-Felder samt deren Häufigkeit ausgegeben.
    """
    if not rest_url.endswith("/"):
        rest_url += "/"
    
    all_notes = []
    start = 0
    count = 100
    total_to_fetch = 500
    # Verwende "id:*" als Query, um alle Notizen abzurufen.
    query_clause = "id:*"
    
    while len(all_notes) < total_to_fetch:
        endpoint = (
            f"{rest_url}search/Note?BhRestToken={bhrest_token}"
            f"&fields=id,action,dateAdded"
            f"&query={query_clause}&sort=dateAdded%3Adesc&start={start}&count={count}"
        )
        print(f"📅 Abrufe Notizen (Start: {start})")
        headers = {"Accept": "application/json"}
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            print(f"❌ Fehler beim Abrufen der Notizen: {response.status_code}")
            print(response.text)
            break
        data = response.json()
        notes = data.get("data", [])
        if not notes:
            print("✅ Keine weiteren Notizen gefunden.")
            break
        all_notes.extend(notes)
        start += count
    
    print(f"✅ Insgesamt {len(all_notes)} Notizen abgerufen.")
    
    # Zähle die verschiedenen action-Werte
    action_counter = {}
    for note in all_notes:
        act = note.get("action") or "None"
        action_counter[act] = action_counter.get(act, 0) + 1
    
    print("\n--- Übersicht der action-Felder (letzte 500 Notizen) ---")
    print("{:<40} {:<10}".format("Action", "Count"))
    print("-" * 50)
    for act, cnt in sorted(action_counter.items()):
        print("{:<40} {:<10}".format(act, cnt))
    
    # Speichern als Debug-Datei
    with open("debug_meetings.json", "w", encoding="utf-8") as f:
        json.dump({"data": all_notes}, f, indent=4)
    print("\n📝 Debug-Datei 'debug_meetings.json' wurde erstellt.")


def main():
    try:
        # Bullhorn: Refresh Token abrufen
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
    
    # Bullhorn: Access Token abrufen
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
    
    # Abrufe die letzten 500 Notizen und erstelle eine Übersicht der action-Felder
    debug_actions_table(bhrest_token, rest_url)


if __name__ == "__main__":
    try:
        main()
        print("🎉 Debug-Prozess abgeschlossen.")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTPError: {str(http_err)}")
    except Exception as e:
        print(f"❌ Fehler: {e}")
