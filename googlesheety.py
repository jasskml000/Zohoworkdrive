import requests
import json
import re
import time
import os
from dotenv import load_dotenv
import base64
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Load environment variables
load_dotenv()

# Base URL and headers for Zoho WorkDrive
base_url = "https://www.zohoapis.in/workdrive/api/v1"
zoho_auth_token = None
zoho_headers = {
    'Authorization': None,
    'Content-Type': 'application/json'
}

# Token refresh configuration
refresh_url_template = "https://accounts.zoho.in/oauth/v2/token?refresh_token={ZOHO_REFRESH_TOKEN}&client_id={ZOHO_CLIENT_ID}&client_secret={ZOHO_CLIENT_SECRET}&grant_type=refresh_token"
refresh_params = {
    "ZOHO_REFRESH_TOKEN": os.getenv("ZOHO_REFRESH_TOKEN"),
    "ZOHO_CLIENT_ID": os.getenv("ZOHO_CLIENT_ID"),
    "ZOHO_CLIENT_SECRET": os.getenv("ZOHO_CLIENT_SECRET")
}
refresh_url = refresh_url_template.format(**refresh_params)
refresh_headers = {
    'Cookie': '_zcsr_tmp=9331a20d-bd0c-45ef-8b49-e2192e15d1e2; iamcsr=9331a20d-bd0c-45ef-8b49-e2192e15d1e2; zalb_6e73717622=dea4bb29906843a6fbdf3bd5c0e43d1d'
}

# Gemini API configuration
gemini_api_key = os.getenv("GEMINI_API_KEY")
gemini_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
gemini_headers = {
    "Content-Type": "application/json",
    "x-goog-api-key": gemini_api_key
}

# Google Sheets configuration
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
SHEET_NAME = "Sheet1"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_PATH='etc/secrets/token.json'

def get_google_sheets_service():
    try:
        creds = None
        if os.path.exists(TOKEN_PATH):
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        if not creds or not creds.valid:
            print("Invalid or missing Google token at %s. Please update google_token.json secret.", TOKEN_PATH)
            raise ValueError("Invalid or missing Google token")
        return build('sheets', 'v4', credentials=creds)
    except Exception as e:
        print(f"Failed to initialize Google Sheets service: {e}")
        raise

def refresh_access_token():
    global zoho_auth_token, zoho_headers
    if None in refresh_params.values():
        print("Error: Missing environment variables for ZOHO_REFRESH_TOKEN, ZOHO_CLIENT_ID, or ZOHO_CLIENT_SECRET")
        return False
    if not gemini_api_key:
        print("Error: Missing environment variable for GEMINI_API_KEY")
        return False
    if not SPREADSHEET_ID:
        print("Error: Missing environment variable for GOOGLE_SPREADSHEET_ID")
        return False
    try:
        response = requests.post(refresh_url, headers=refresh_headers)
        response.raise_for_status()
        data = response.json()
        if 'access_token' in data:
            zoho_auth_token = f"Zoho-oauthtoken {data['access_token']}"
            zoho_headers['Authorization'] = zoho_auth_token
            print(f"New access token obtained: {zoho_auth_token}")
            return True
        else:
            print(f"Failed to refresh token: {data.get('error', 'Unknown error')}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error refreshing token: {e}")
        return False

def fetch_file_ids():
    file_ids = []
    limit = 50
    offset = 0
    while True:
        url = f"{base_url}/files/brz9daf6f78081d2f4d33b0621e4fbf41167d/files?page[limit]={limit}&page[offset]={offset}"
        try:
            response = requests.get(url, headers=zoho_headers)
            response.raise_for_status()
            data = response.json()
            files = data.get('data', [])
            if not files:
                break
            for file in files:
                file_id = file.get('id')
                if file_id:
                    file_ids.append(file_id)
            if len(files) < limit:
                break
            offset += limit
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching files: {e}")
            break
    return file_ids

def set_permissions_and_get_permalink(file_id):
    url = f"{base_url}/permissions"
    payload = json.dumps({
        "data": {
            "attributes": {
                "resource_id": file_id,
                "shared_type": "everyone",
                "role_id": "34"
            },
            "type": "permissions"
        }
    })
    try:
        response = requests.post(url, headers=zoho_headers, data=payload)
        response.raise_for_status()
        response_data = response.json()
        print(f"Permissions set for file {file_id}: {json.dumps(response_data, indent=2)}")
        
        # Directly extract permalink
        permalink = response_data.get('data', {}).get('attributes', {}).get('permalink')
        if permalink:
            print(f"Directly extracted permalink for file {file_id}: {permalink}")
        else:
            print(f"No permalink found directly for file {file_id}, using Gemini API...")
            permalink = extract_permalink_with_gemini(response_data)
            print(f"Gemini extracted permalink for file {file_id}: {permalink}")
        
        return {"file_id": file_id, "permalink": permalink, "success": True}
    except requests.exceptions.RequestException as e:
        print(f"Error processing file {file_id}: {e}")
        return {"file_id": file_id, "permalink": None, "success": False}

def extract_permalink_with_gemini(response_data):
    response_text = json.dumps(response_data, indent=2)
    prompt = f"""
    You are given a JSON response from an API. Extract the value of the 'permalink' field, which should be a URL string, from the JSON.
    Return only the URL as a plain string, without any markdown, backticks, or extra text.
    If the 'permalink' field is missing or empty, return the string 'None'.
    JSON: {response_text}
    """
    
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3}
    })
    
    try:
        response = requests.post(gemini_url, headers=gemini_headers, data=payload)
        response.raise_for_status()
        gemini_response = response.json()
        extracted_text = gemini_response.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', 'None')
        
        extracted_text = extracted_text.strip()
        if extracted_text == 'None' or not extracted_text:
            print(f"No permalink extracted by Gemini for file")
            return None
        if not re.match(r'^https?://', extracted_text):
            print(f"Invalid URL extracted by Gemini: {extracted_text}")
            return None
        print(f"Gemini extracted permalink: {extracted_text}")
        return extracted_text
    except requests.exceptions.RequestException as e:
        print(f"Error calling Gemini API: {e}")
        return None

def append_to_google_sheet(results):
    try:
        service = get_google_sheets_service()
        sheet = service.spreadsheets()
        
        # Read existing file IDs from column A (assuming headers in row 1)
        range_name = f"{SHEET_NAME}!A2:A"
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
        existing_ids = set(value[0] for value in result.get('values', []) if value)
        print(f"Existing file IDs in Google Sheet: {existing_ids}")

        # Filter new results and prepare data to append
        values_to_append = []
        for file_id, permalink in results:
            if file_id not in existing_ids:
                values_to_append.append([file_id, permalink])
                print(f"Will append file ID {file_id} with permalink {permalink}")
            else:
                print(f"Skipping file ID {file_id} as it already exists in Google Sheet")

        if not values_to_append:
            print("No new file IDs to append.")
            return

        # Append new data
        body = {
            'values': values_to_append
        }
        result = sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A2:B",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        print(f"Appended {result.get('updates', {}).get('updatedRows', 0)} rows to Google Sheet")
    except HttpError as e:
        print(f"Error appending to Google Sheet: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def main():
    # Refresh access token
    print("Refreshing access token...")
    if not refresh_access_token():
        print("Failed to refresh access token. Exiting.")
        return
    
    print("Fetching file IDs...")
    file_ids = fetch_file_ids()
    print(f"Found {len(file_ids)} files.")

    results = []
    for file_id in file_ids:
        print(f"\nProcessing file ID: {file_id}")
        result = set_permissions_and_get_permalink(file_id)
        if result["success"] and result["permalink"]:
            results.append([result["file_id"], result["permalink"]])
        time.sleep(1)

    if results:
        append_to_google_sheet(results)
    else:
        print("No valid results to append to Google Sheet.")

if __name__ == "__main__":
    main()
