import requests
import os
from urllib.parse import quote_plus
from urllib.parse import unquote

# Zugangsdaten (für GitHub Actions: per Umgebungsvariablen setzen)
CLIENT_ID = '368d46ec-e680-4bba-9e46-9f4e3d1a9fba'
CLIENT_SECRET = 'Dzqsclojm3eYPXYiSYoAxrIW'
USERNAME = 'corporateconnect.powerbi'
PASSWORD = '0a4&3oJW22xZ5rb('
REDIRECT_URI = 'https://welcome.bullhornstaffing.com'
SWIMLANE = 'ger'

HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

def get_access_and_refresh_token(authorization_code):
    """ Holt Access Token und Refresh Token mithilfe des Authorization Codes. """
    token_url = f"https://auth-{SWIMLANE}.bullhornstaffing.com/oauth/token"
    payload = {
        "grant_type": "authorization_code",
        "code": authorization_code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI
    }
    response = requests.post(token_url, data=payload, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"❌ Fehler bei Access-Token-Abruf: {response.json()}")
    tokens = response.json()
    print("✅ Access Token:", tokens.get("access_token"))
    print("🔄 Refresh Token:", tokens.get("refresh_token"))
    return tokens


def refresh_access_token(refresh_token):
    """ Holt einen neuen Access Token mit dem Refresh Token. """
    token_url = f"https://auth-{SWIMLANE}.bullhornstaffing.com/oauth/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(token_url, data=payload, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"❌ Fehler beim Refresh-Token-Abruf: {response.json()}")
    tokens = response.json()
    print("🔄 Neuer Access Token:", tokens.get("access_token"))
    return tokens


def get_bhrest_token(access_token):
    """ Holt BhRestToken und restUrl basierend auf dem Access Token. """
    login_url = (
        f"https://rest-{SWIMLANE}.bullhornstaffing.com/rest-services/login?version=*"
        f"&access_token={quote_plus(access_token)}"
    )
    response = requests.post(login_url)
    if response.status_code != 200:
        raise Exception(f"❌ Fehler beim BhRestToken-Abruf: {response.json()}")
    login_info = response.json()
    print("🗝️  BhRestToken:", login_info.get("BhRestToken"))
    print("🌐 REST URL:", login_info.get("restUrl"))
    return login_info


if __name__ == "__main__":
    # Schritt 1: Authorization Code manuell bereitstellen
    print("⚠️  Bitte generiere den Authorization Code manuell über die OAuth-URL und füge ihn hier ein.")
    auth_code = input("\n🔑 Füge hier den Authorization Code ein: ")
    auth_code = unquote(auth_code)

    # Schritt 2: Access + Refresh Token abrufen
    tokens = get_access_and_refresh_token(auth_code)

    # Schritt 3: BhRestToken abrufen
    get_bhrest_token(tokens.get("access_token"))

    print("✅ Prozess abgeschlossen. Access-, Refresh- und BhRestToken erfolgreich abgerufen.")