import os, io, requests
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from msal import ConfidentialClientApplication

slack_bot_token = os.environ["SLACK_BOT_TOKEN"]
channel_id = os.environ["SLACK_CHANNEL_ID"]
tenant_id = os.environ["AZURE_TENANT_ID"]
client_id = os.environ["AZURE_CLIENT_ID"]
client_secret = os.environ["AZURE_CLIENT_SECRET"]
user_principal_name = os.environ["ONEDRIVE_UPN"]
onedrive_file_path = os.environ.get("ONEDRIVE_FILE_PATH", "/Documentos/tickets_slack.xlsx")
target_hour_local = int(os.environ.get("TARGET_HOUR_LOCAL", "19"))

dev_team_member_ids = [i.strip() for i in os.environ.get("DEV_TEAM_MEMBER_IDS","").split(",") if i.strip()]

def now_scl():
    return datetime.now(tz=ZoneInfo("America/Santiago"))

def should_run():
    n = now_scl()
    return n.hour == target_hour_local

def token_graph():
    app = ConfidentialClientApplication(client_id=client_id, client_credential=client_secret, authority=f"https://login.microsoftonline.com/{tenant_id}")
    r = app.acquire_token_silent(scopes=["https://graph.microsoft.com/.default"], account=None)
    if not r:
        r = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in r:
        raise RuntimeError("Graph token")
    return r["access_token"]

def gget(url, token):
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=h)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {r.status_code}")
    return r

def gput(url, token, data, content_type):
    h = {"Authorization": f"Bearer {token}", "Content-Type": content_type}
    r = requests.put(url, headers=h, data=data)
    if r.status_code >= 400:
        raise RuntimeError(f"PUT {r.status_code}")
    return r

def ensure_file(token):
    meta = f"https://graph.microsoft.com/v1.0/users/{user_principal_name}/drive/root:{onedrive_file_path}"
    r = requests.get(meta, headers={"Authorization": f"Bearer {token}"})
    if r.status_code == 404:
        buf = io.BytesIO()
        wb = Workbook()
        ws = wb.active
        ws.title = "TMP"
        wb.save(buf)
        buf.seek(0)
        upload = f"https://graph.microsoft.com/v1.0/users/{user_principal_name}/drive/root:{onedrive_file_path}:/content"
        gput(upload, token, buf.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    elif r.status_code >= 400:
        raise RuntimeError("OneDrive meta")

def dl_excel(token):
    url = f"https://graph.microsoft.com/v1.0/users/{user_principal_name}/drive/root:{onedrive_file_path}:/content"
    r = gget(url, token)
    return io.BytesIO(r.content)

def up_excel(token, bio):
    url = f"https://graph.microsoft.com/v1.0/users/{user_principal_name}/drive/root:{onedrive_file_path}:/content"
    gput(url, token, bio.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def fetch_messages():
    c = WebClient(token=slack_bot_token)
    out = []
    cur = None
    while True:
        try:
            res = c.conversations_history(channel=channel_id, limit=1000, cursor=cur)
            out.extend(res.get("messages", []))
            cur = res.get("response_metadata", {}).get("next_cursor")
            if not cur:
                break
        except SlackApiError as e:
            raise RuntimeError(f"Slack {e.response['error']}")
    return out

def tz_dt(ts):
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(ZoneInfo("America/Santiago"))

def build_df(msgs):
    datos = []
    for m in reversed(msgs):
        uid = m.get("user")
        if not uid:
            continue
        ts = m.get("ts")
        dt = tz_dt(ts)
        origen = "Producto" if uid in dev_team_member_ids else "Otras áreas"
        datos.append({
            "Fecha aproximada": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "Origen": origen,
            "SLACK": m.get("text", "")
        })
    cols = ["Fecha aproximada","Origen","SLACK"]
    if not datos:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(datos, columns=cols)

def append_rows(wb, df):
    for hoja, dfh in [("Datos", df)]:
        if hoja not in wb.sheetnames:
            ws = wb.create_sheet(title=hoja)
            ws.append(list(dfh.columns))
        else:
            ws = wb[hoja]
            if ws.max_row == 1 and [c.value for c in ws[1]] != list(dfh.columns):
                ws.delete_rows(1, ws.max_row)
                ws.append(list(dfh.columns))
        for _, r in dfh.iterrows():
            ws.append(list(r.values))
    if "Sheet" in wb.sheetnames and wb["Sheet"].max_row == 1 and wb["Sheet"].max_column == 1 and wb["Sheet"]["A1"].value is None:
        wb.remove(wb["Sheet"])

def main():
    if not should_run():
        return
    token = token_graph()
    ensure_file(token)
    msgs = fetch_messages()
    df = build_df(msgs)
    if df.empty:
        return
    bio = dl_excel(token)
    try:
        wb = load_workbook(bio)
    except:
        wb = Workbook()
        wb.active.title = "TMP"
    append_rows(wb, df)
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    up_excel(token, out)

if __name__ == "__main__":
    main()
