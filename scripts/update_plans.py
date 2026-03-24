import os
import json
import base64
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GOOGLE_CREDS   = os.environ["GOOGLE_CREDS"]
GITHUB_REPO    = "hirschhornor-glitch/oranim-map"
SHEET_NAME     = "Oranim_Taba"
KEY_FIELD      = "plan_name"
TS_FIELD       = "last_modified"
TIMESTAMP_FILE = "data/last_update.txt"

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).sheet1

def get_github_file(path):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        data = r.json()
        content = base64.b64decode(data["content"]).decode("utf-8")
        return data["sha"], content
    return None, None

def upload_github_file(path, content, sha, message):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=HEADERS, json=payload)
    return r.status_code in (200, 201)

def load_last_update():
    _, content = get_github_file(TIMESTAMP_FILE)
    if content:
        try:
            return datetime.strptime(content.strip(), "%Y-%m-%d %H:%M:%S")
        except:
            pass
    return datetime.min

def save_last_update():
    sha, _ = get_github_file(TIMESTAMP_FILE)
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    upload_github_file(TIMESTAMP_FILE, now_str, sha,
                       f"update timestamp {now_str}")

def update_plans():
    last_update = load_last_update()
    print(f"עדכון אחרון: {last_update}")

    sheet = get_sheet()
    all_rows = sheet.get_all_records()

    changed_rows = {}
    for row in all_rows:
        ts_str = row.get(TS_FIELD, "")
        if not ts_str:
            continue
        try:
            ts = datetime.strptime(str(ts_str), "%Y-%m-%d %H:%M:%S")
        except:
            continue
        if ts > last_update:
            changed_rows[str(row[KEY_FIELD])] = row

    if not changed_rows:
        print("אין שינויים מאז העדכון האחרון")
        return

    print(f"נמצאו {len(changed_rows)} שורות שהשתנו")

    sha, existing_geojson = get_github_file("data/plans.geojson")
    if not existing_geojson:
        print("לא נמצא plans.geojson ב-GitHub")
        return

    geojson_data = json.loads(existing_geojson)

    updated = 0
    for feature in geojson_data["features"]:
        plan_name = str(feature["properties"].get(KEY_FIELD, ""))
        if plan_name in changed_rows:
            for k, v in changed_rows[plan_name].items():
                feature["properties"][k] = v
            updated += 1

    print(f"מעדכן {updated} פיצ'רים...")
    geojson_str = json.dumps(geojson_data, ensure_ascii=False)
    success = upload_github_file(
        "data/plans.geojson", geojson_str, sha,
        f"update plans {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
    )

    if success:
        print("✓ plans.geojson עודכן")
        save_last_update()
    else:
        print("✗ שגיאה בעדכון")

update_plans()
