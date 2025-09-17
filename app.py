import os, io, requests, pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import msal

slack_bot_token=os.environ["SLACK_BOT_TOKEN"]
channel_id=os.environ["SLACK_CHANNEL_ID"]
client_id=os.environ["AZURE_CLIENT_ID"]
refresh_token=os.environ["GRAPH_REFRESH_TOKEN"]
onedrive_upn=os.environ["ONEDRIVE_UPN"]
onedrive_file_path=os.environ.get("ONEDRIVE_FILE_PATH","/Documents/KPI MQ.xlsx")
target_hour_local=int(os.environ.get("TARGET_HOUR_LOCAL","19"))
dev_team_member_ids=[i.strip() for i in os.environ.get("DEV_TEAM_MEMBER_IDS","").split(",") if i.strip()]
debug_mode=os.environ.get("DEBUG_MODE","0")=="1"

def now_scl():
    return datetime.now(tz=ZoneInfo("America/Santiago"))

def should_run():
    n=now_scl()
    return n.hour==target_hour_local

def acquire_token():
    data={
        "client_id":client_id,
        "refresh_token":refresh_token,
        "grant_type":"refresh_token",
        "scope":"offline_access Files.ReadWrite"
    }
    r=requests.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token",data=data)
    if r.status_code>=400:
        raise RuntimeError("token")
    return r.json()["access_token"]

def gget(url,token):
    r=requests.get(url,headers={"Authorization":f"Bearer {token}"})
    if r.status_code>=400:
        raise RuntimeError("get")
    return r

def gput(url,token,data,content_type):
    r=requests.put(url,headers={"Authorization":f"Bearer {token}","Content-Type":content_type},data=data)
    if r.status_code>=400:
        print(f"[ERROR] Error en PUT: {r.status_code} - {r.text}")
        raise RuntimeError("put")
    return r

def ensure_file(token):
    meta=f"https://graph.microsoft.com/v1.0/users/{onedrive_upn}/drive/root:{onedrive_file_path}"
    r=requests.get(meta,headers={"Authorization":f"Bearer {token}"})
    if r.status_code==404:
        buf=io.BytesIO()
        wb=Workbook()
        ws=wb.active
        ws.title="TMP"
        wb.save(buf)
        buf.seek(0)
        upload=f"https://graph.microsoft.com/v1.0/users/{onedrive_upn}/drive/root:{onedrive_file_path}:/content"
        gput(upload,token,buf.getvalue(),"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        print(f"[WARN] Archivo no existía, creado nuevo en {onedrive_file_path}")
    elif r.status_code>=400:
        raise RuntimeError("meta")

def dl_excel(token):
    url=f"https://graph.microsoft.com/v1.0/users/{onedrive_upn}/drive/root:{onedrive_file_path}:/content"
    return io.BytesIO(gget(url,token).content)

def up_excel(token,bio):
    url=f"https://graph.microsoft.com/v1.0/users/{onedrive_upn}/drive/root:{onedrive_file_path}:/content"
    try:
        gput(url,token,bio.getvalue(),"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        print(f"[INFO] Archivo subido exitosamente a OneDrive")
    except Exception as e:
        print(f"[ERROR] Error al subir archivo: {e}")
        # Guardar archivo localmente como respaldo
        backup_filename = f"backup_kpi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with open(backup_filename, 'wb') as f:
            f.write(bio.getvalue())
        print(f"[INFO] Archivo guardado localmente como respaldo: {backup_filename}")
        raise

def fetch_messages(oldest=None, latest=None):
    c=WebClient(token=slack_bot_token)
    out=[]
    cur=None
    while True:
        res=c.conversations_history(
            channel=channel_id,
            limit=1000,
            cursor=cur,
            oldest=oldest,
            latest=latest
        )
        out.extend(res.get("messages",[]))
        cur=res.get("response_metadata",{}).get("next_cursor")
        if not cur:
            break
    return out

def tz_dt(ts):
    return datetime.fromtimestamp(float(ts),tz=timezone.utc).astimezone(ZoneInfo("America/Santiago"))

def build_df(msgs):
    datos=[]
    for m in reversed(msgs):
        uid=m.get("user")
        if not uid:
            continue
        dt=tz_dt(m.get("ts"))
        origen="Producto" if uid in dev_team_member_ids else "Otras áreas"
        datos.append({
            "Fecha aproximada":dt.strftime("%Y-%m-%d %H:%M:%S"),
            "Origen":origen,
            "SLACK":m.get("text",""),
            "Diagnóstico causa raíz":"",
            "Propuesta (Tarea en ClickUp cuando sea desarrollable /Cambio sistema)":"",
            "ESTADO FINAL":""
        })
    cols=["Fecha aproximada","Origen","SLACK","Diagnóstico causa raíz","Propuesta (Tarea en ClickUp cuando sea desarrollable /Cambio sistema)","ESTADO FINAL"]
    return pd.DataFrame(datos,columns=cols) if datos else pd.DataFrame(columns=cols)

def get_month_name_from_period(df):
    """Obtiene el nombre del mes del primer día del período"""
    if df.empty:
        return "Datos"
    first_date = df.iloc[0]["Fecha aproximada"]
    dt = datetime.strptime(first_date, "%Y-%m-%d %H:%M:%S")
    month_names = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                   "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    return month_names[dt.month - 1]

def apply_table_style(ws, num_rows):
    """Aplica estilo profesional a la tabla"""
    if num_rows <= 1:  # Solo header o sin datos
        return
    
    # Definir estilos
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    
    data_font = Font(size=10)
    data_alignment = Alignment(vertical="top", wrap_text=True)
    
    # Borde para todas las celdas
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Aplicar estilo al header (fila 1)
    for col in range(1, ws.max_column + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
        cell.border = thin_border
    
    # Aplicar estilo a las filas de datos
    for row in range(2, num_rows + 1):
        for col in range(1, ws.max_column + 1):
            cell = ws.cell(row=row, column=col)
            cell.font = data_font
            cell.alignment = data_alignment
            cell.border = thin_border
    
    # Ajustar ancho de columnas
    column_widths = {
        'A': 20,  # Fecha aproximada
        'B': 15,  # Origen
        'C': 50,  # SLACK
        'D': 30,  # Diagnóstico causa raíz
        'E': 40,  # Propuesta
        'F': 20   # ESTADO FINAL
    }
    
    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width
    
    # Ajustar altura de filas
    for row in range(1, num_rows + 1):
        ws.row_dimensions[row].height = 30

def append_rows(wb,df):
    if df.empty:
        return
    
    hoja = get_month_name_from_period(df)
    
    # Verificar si la hoja ya existe
    if hoja not in wb.sheetnames:
        ws=wb.create_sheet(title=hoja)
        ws.append(list(df.columns))
    else:
        ws=wb[hoja]
        if ws.max_row==1 and [c.value for c in ws[1]]!=list(df.columns):
            ws.delete_rows(1,ws.max_row)
            ws.append(list(df.columns))
    
    # Obtener mensajes existentes para verificar duplicados
    existing_messages = set()
    if ws.max_row > 1:  # Si hay datos además del header
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=True):
            if len(row) >= 3 and row[2]:  # SLACK column (índice 2)
                existing_messages.add(str(row[2]).strip())
    
    # Agregar solo mensajes nuevos
    new_rows_added = 0
    for _,r in df.iterrows():
        slack_message = str(r["SLACK"]).strip()
        if slack_message and slack_message not in existing_messages:
            ws.append(list(r.values))
            existing_messages.add(slack_message)
            new_rows_added += 1
    
    print(f"[INFO] Filas nuevas agregadas: {new_rows_added} (duplicados ignorados: {len(df) - new_rows_added})")
    
    # Aplicar estilo a la tabla
    if new_rows_added > 0:
        apply_table_style(ws, ws.max_row)
        print(f"[INFO] Estilo aplicado a la tabla")
    
    if "Sheet" in wb.sheetnames and wb["Sheet"].max_row==1 and wb["Sheet"].max_column==1 and wb["Sheet"]["A1"].value is None:
        wb.remove(wb["Sheet"])

def main():
    print(f"[INFO] Inicio ejecución: {now_scl()}")
    token=acquire_token()
    print("[INFO] Access token obtenido")

    ensure_file(token)

    first_run_flag=".first_run_done"
    if not os.path.exists(first_run_flag):
        start_dt=datetime(2025,9,11,tzinfo=timezone.utc)
        oldest=str(start_dt.timestamp())
        latest=str(datetime.now(tz=timezone.utc).timestamp())
        print(f"[INFO] Primera corrida: desde {start_dt} hasta ahora")
        msgs=fetch_messages(oldest=oldest, latest=latest)
        open(first_run_flag,"w").close()
    else:
        if not (should_run() or debug_mode):
            print(f"[INFO] No es hora ({now_scl().hour}), ni debug, saliendo")
            return
        print("[INFO] Buscando mensajes recientes en Slack")
        msgs=fetch_messages()

    print(f"[INFO] Mensajes obtenidos: {len(msgs)}")
    df=build_df(msgs)
    if df.empty:
        print("[INFO] No hay mensajes nuevos")
        return
    print(f"[INFO] Filas a agregar: {len(df)}")
    print("[DEBUG] Preview:\n", df.head(5).to_string())

    bio=dl_excel(token)
    try:
        wb=load_workbook(bio)
        print("[INFO] Excel cargado")
    except:
        wb=Workbook()
        wb.active.title="TMP"
        print("[WARN] Excel nuevo creado")

    append_rows(wb,df)
    if not df.empty:
        sheet_name = get_month_name_from_period(df)
        print(f"[INFO] Datos procesados en hoja '{sheet_name}'")

    out=io.BytesIO()
    wb.save(out)
    out.seek(0)
    up_excel(token,out)
    print(f"[INFO] Excel actualizado en OneDrive: {onedrive_file_path}")

    print(f"[INFO] Fin ejecución: {now_scl()}")

if __name__=="__main__":
    main()
