import os, io, requests, pandas as pd
from datetime import timedelta
from slack_sdk import WebClient
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import msal
import re
import time

slack_bot_token=os.environ["SLACK_BOT_TOKEN"]
channel_id=os.environ["SLACK_CHANNEL_ID"]
client_id=os.environ["AZURE_CLIENT_ID"]
refresh_token=os.environ.get("GRAPH_REFRESH_TOKEN","")
onedrive_upn=os.environ["ONEDRIVE_UPN"]
onedrive_file_path=os.environ.get("ONEDRIVE_FILE_PATH","/Documents/BlackBox.xlsx")
# target_hour_local eliminado - ya no se usa restricción de hora
dev_team_member_ids=[i.strip() for i in os.environ.get("DEV_TEAM_MEMBER_IDS","").split(",") if i.strip()]
debug_mode=os.environ.get("DEBUG_MODE","0")=="1"
refresh_token_path=os.environ.get("REFRESH_TOKEN_PATH","/data/graph_refresh_token")
device_flow_wait_seconds=int(os.environ.get("DEVICE_FLOW_WAIT_SECONDS","600"))  # 10 min
graph_scope=os.environ.get("GRAPH_SCOPE","offline_access Files.ReadWrite").strip() or "offline_access Files.ReadWrite"

def now_scl():
    return datetime.now(tz=ZoneInfo("America/Santiago"))

# Función should_run() eliminada - ya no se usa restricción de hora

def load_refresh_token() -> str:
    """Lee refresh token desde archivo persistente (si existe) o desde ENV."""
    try:
        p=(refresh_token_path or "").strip()
        if p and os.path.exists(p):
            with open(p,"r",encoding="utf-8") as f:
                t=f.read().strip()
                if t:
                    return t
    except Exception as e:
        print(f"[WARN] No se pudo leer REFRESH_TOKEN_PATH={refresh_token_path}: {e}")
    return (os.environ.get("GRAPH_REFRESH_TOKEN","").strip() or "")

def save_refresh_token(token: str):
    """Guarda refresh token en archivo persistente para próximos runs."""
    if not token:
        return
    try:
        p=(refresh_token_path or "").strip()
        if not p:
            return
        d=os.path.dirname(p)
        if d:
            os.makedirs(d,exist_ok=True)
        with open(p,"w",encoding="utf-8") as f:
            f.write(token.strip())
        print(f"[INFO] Refresh token guardado en {p}")
    except Exception as e:
        print(f"[WARN] No se pudo guardar refresh token en {refresh_token_path}: {e}")

def acquire_token():
    tenant = os.environ.get("AZURE_TENANT", "consumers").strip() or "consumers"
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

    if not client_id:
        raise RuntimeError("token: falta variable de entorno AZURE_CLIENT_ID")

    rt=load_refresh_token() or refresh_token

    def device_flow_token():
        device_url=f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/devicecode"
        dc=requests.post(device_url,data={"client_id":client_id,"scope":graph_scope},timeout=30)
        if dc.status_code>=400:
            raise RuntimeError(f"token: devicecode falló (status={dc.status_code}): {dc.text[:1000]}")
        flow=dc.json()
        msg=flow.get("message") or (
            f"Autorización requerida.\n1) Abrí {flow.get('verification_uri')}\n2) Ingresá el código: {flow.get('user_code')}"
        )
        print("[WARN] Se requiere re-login de Microsoft. Device Code Flow:")
        print(msg)
        # No enviar mensajes a Slack: solo log.

        interval=int(flow.get("interval",5))
        deadline=time.time()+min(int(flow.get("expires_in",900)),device_flow_wait_seconds)
        while time.time()<deadline:
            time.sleep(interval)
            tr=requests.post(
                token_url,
                data={
                    "grant_type":"urn:ietf:params:oauth:grant-type:device_code",
                    "client_id":client_id,
                    "device_code":flow.get("device_code"),
                },
                timeout=30
            )
            if tr.status_code==200:
                tok=tr.json()
                new_rt=tok.get("refresh_token")
                if new_rt:
                    save_refresh_token(new_rt)
                at=tok.get("access_token")
                if not at:
                    raise RuntimeError(f"token: device flow sin access_token: {str(tok)[:800]}")
                return at

            # Respuestas esperables mientras se autoriza
            try:
                err=tr.json()
            except ValueError:
                raise RuntimeError(f"token: device flow respuesta no-JSON (status={tr.status_code}): {tr.text[:500]}")
            code=err.get("error")
            if code in {"authorization_pending","slow_down"}:
                if code=="slow_down":
                    interval+=5
                continue
            raise RuntimeError(f"token: device flow falló: {err}")

        raise RuntimeError("token: expiró la espera de autorización (device flow). Reintentá luego de autorizar.")

    # Si no hay refresh token, necesitamos device flow sí o sí.
    if not rt:
        return device_flow_token()

    # Para refresh_token en v2.0, el parámetro scope es opcional; si se incluye, debe ser subconjunto del original.
    # Intentamos primero con scope (comportamiento actual) y, si falla con error de scope, reintentamos sin scope.
    base_data = {
        "client_id": client_id,
        "refresh_token": rt,
        "grant_type": "refresh_token",
    }
    attempts = [
        {**base_data, "scope": "offline_access Files.ReadWrite"},
        base_data,
    ]

    last_err = None
    for data in attempts:
        try:
            r = requests.post(token_url, data=data, timeout=30)
        except requests.RequestException as e:
            last_err = f"token: error de red llamando a {token_url}: {e}"
            continue

        if r.status_code < 400:
            try:
                payload = r.json()
            except ValueError:
                raise RuntimeError(f"token: respuesta no-JSON (status {r.status_code}) desde {token_url}: {r.text[:500]}")

            access_token = payload.get("access_token")
            if not access_token:
                raise RuntimeError(f"token: respuesta sin access_token desde {token_url}: {str(payload)[:800]}")
            return access_token

        # Error HTTP: extraer detalles (sin imprimir secretos)
        try:
            err = r.json()
        except ValueError:
            err = {"raw": r.text[:1000]}

        error_code = err.get("error") or "unknown_error"
        error_desc = err.get("error_description") or err.get("raw") or ""
        request_id = r.headers.get("request-id") or r.headers.get("x-ms-request-id") or r.headers.get("client-request-id") or ""

        last_err = (
            "token: fallo al refrescar access token "
            f"(tenant={tenant}, status={r.status_code}, error={error_code})"
            + (f", request_id={request_id}" if request_id else "")
            + (f": {error_desc}" if error_desc else "")
        )

        # Si el error sugiere un problema de scope, probamos el siguiente intento (sin scope).
        if error_code in {"invalid_scope"} or "scope" in str(error_desc).lower():
            continue
        # Si el refresh token quedó inválido (invalid_grant), caemos al Device Code Flow.
        # En cuentas personales esto puede ocurrir periódicamente y requiere re-login.
        if error_code=="invalid_grant":
            return device_flow_token()
        break

    # Fallback extra: si por algún motivo no matcheó antes, pero el último error fue invalid_grant, intentar device flow.
    if last_err and "error=invalid_grant" in last_err:
        return device_flow_token()

    raise RuntimeError(last_err or "token: fallo desconocido al refrescar access token")

def gget(url,token):
    r=requests.get(url,headers={"Authorization":f"Bearer {token}"})
    if r.status_code>=400:
        raise RuntimeError("get")
    return r

def gput(url,token,data,content_type):
    r=requests.put(url,headers={"Authorization":f"Bearer {token}","Content-Type":content_type},data=data)
    if r.status_code>=400:
        print(f"[ERROR] Error en PUT: {r.status_code} - {r.text}")
        # No lanzar excepción para errores 423 (archivo bloqueado) o 409 (conflicto)
        if r.status_code in [423, 409]:
            print(f"[WARN] Archivo bloqueado o en conflicto, continuando...")
            return r
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
        return True
    except Exception as e:
        print(f"[ERROR] Error al subir archivo: {e}")
        
        # Si el archivo está bloqueado (423) o en conflicto (409), crear una copia con timestamp
        if "423" in str(e) or "409" in str(e):
            try:
                # Crear nombre de archivo con timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                # Obtener el directorio y nombre base del archivo original
                base_path = onedrive_file_path.rsplit('/', 1)[0] if '/' in onedrive_file_path else ""
                base_name = onedrive_file_path.rsplit('/', 1)[-1].rsplit('.', 1)[0] if '.' in onedrive_file_path else onedrive_file_path
                extension = onedrive_file_path.rsplit('.', 1)[-1] if '.' in onedrive_file_path else "xlsx"
                
                # Crear nueva ruta con timestamp
                backup_path = f"{base_path}/{base_name}_backup_{timestamp}.{extension}" if base_path else f"{base_name}_backup_{timestamp}.{extension}"
                backup_url = f"https://graph.microsoft.com/v1.0/users/{onedrive_upn}/drive/root:{backup_path}:/content"
                
                # Subir la copia
                gput(backup_url, token, bio.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                print(f"[INFO] Archivo bloqueado - copia creada en OneDrive: {backup_path}")
                return True
                
            except Exception as backup_error:
                print(f"[ERROR] Error al crear copia en OneDrive: {backup_error}")
        
        # Guardar archivo localmente como respaldo adicional
        backup_filename = f"backup_blackbox_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        try:
            with open(backup_filename, 'wb') as f:
                f.write(bio.getvalue())
            print(f"[INFO] Archivo guardado localmente como respaldo: {backup_filename}")
        except Exception as backup_error:
            print(f"[WARN] No se pudo guardar respaldo local: {backup_error}")
        
        # No lanzar excepción, solo reportar el error
        print(f"[WARN] Continuando sin subir a OneDrive debido al error")
        return False

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
        
        # Crear enlace clickeable al mensaje de Slack
        ts = m.get("ts", "")
        # Formatear timestamp correctamente para Slack (formato: p1234567890123456)
        ts_formatted = ts.replace('.', '')
        slack_link = f"https://mq-sede.slack.com/archives/{channel_id}/p{ts_formatted}"
        slack_text = m.get("text", "")
        
        # Limpiar texto para evitar caracteres problemáticos en Excel
        if slack_text:
            # Escapar comillas y caracteres especiales
            clean_text = slack_text.replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            # Limitar longitud del texto para evitar problemas
            if len(clean_text) > 200:
                clean_text = clean_text[:197] + "..."
            slack_content = f'=HYPERLINK("{slack_link}","{clean_text}")'
        else:
            slack_content = ""
        
        datos.append({
            "Fecha aproximada":dt.strftime("%Y-%m-%d %H:%M:%S"),
            "Origen":origen,
            "SLACK":slack_content,
            "Diagnóstico causa raíz":"",
            "Propuesta (Tarea en ClickUp cuando sea desarrollable /Cambio sistema)":"",
            "ESTADO FINAL":""
        })
    cols=["Fecha aproximada","Origen","SLACK","Diagnóstico causa raíz","Propuesta (Tarea en ClickUp cuando sea desarrollable /Cambio sistema)","ESTADO FINAL"]
    return pd.DataFrame(datos,columns=cols) if datos else pd.DataFrame(columns=cols)

def extract_hyperlink_url(cell_value):
    """
    Extrae la URL de una fórmula de Excel del tipo:
    =HYPERLINK("url","texto")
    Si no aplica o falla, retorna None.
    """
    if cell_value is None:
        return None
    s = str(cell_value).strip()
    if not s.startswith("=HYPERLINK("):
        return None
    # Captura la primera cadena entre comillas (la URL)
    m = re.match(r'^=HYPERLINK\("([^"]+)"\s*,', s)
    return m.group(1) if m else None

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
    
    try:
        # Definir estilos
        header_font = Font(bold=True, color="FFFFFF", size=12)
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        
        data_font = Font(size=11)
        data_alignment = Alignment(vertical="top", wrap_text=True)
        
        # Borde para todas las celdas
        thin_border = Border(
            left=Side(style='thin', color='000000'),
            right=Side(style='thin', color='000000'),
            top=Side(style='thin', color='000000'),
            bottom=Side(style='thin', color='000000')
        )
        
        # Aplicar estilo al header (fila 1)
        for col in range(1, min(ws.max_column + 1, 7)):  # Limitar a 6 columnas
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border
        
        # Aplicar estilo a las filas de datos
        for row in range(2, min(num_rows + 1, ws.max_row + 1)):
            for col in range(1, min(ws.max_column + 1, 7)):  # Limitar a 6 columnas
                cell = ws.cell(row=row, column=col)
                cell.font = data_font
                cell.alignment = data_alignment
                cell.border = thin_border
        
        # Ajustar ancho de columnas (valores más grandes para evitar tablas pequeñas)
        column_widths = {
            'A': 25,  # Fecha aproximada
            'B': 18,  # Origen
            'C': 60,  # SLACK
            'D': 35,  # Diagnóstico causa raíz
            'E': 45,  # Propuesta
            'F': 25   # ESTADO FINAL
        }
        
        for col_letter, width in column_widths.items():
            ws.column_dimensions[col_letter].width = width
        
        # Ajustar altura de filas (altura más grande para mejor legibilidad)
        max_rows = min(num_rows, 200)  # Aumentar límite
        for row in range(1, max_rows + 1):
            ws.row_dimensions[row].height = 40  # Aumentar altura de filas
        
        # Asegurar que el zoom esté al 100%
        ws.sheet_view.zoomScale = 100
        
        print(f"[INFO] Estilo aplicado: {max_rows} filas, columnas ajustadas")
            
    except Exception as e:
        print(f"[WARN] Error aplicando estilo: {e}")
        # Continuar sin estilo si hay problemas

def append_rows(wb,df):
    if df.empty:
        return
    
    hoja = get_month_name_from_period(df)
    
    # Verificar si la hoja ya existe
    if hoja not in wb.sheetnames:
        ws=wb.create_sheet(title=hoja)
        ws.append(list(df.columns))
        # Aplicar estilo inmediatamente al crear nueva hoja
        apply_table_style(ws, 1)
        print(f"[INFO] Nueva hoja '{hoja}' creada con estilo")
    else:
        ws=wb[hoja]
        if ws.max_row==1 and [c.value for c in ws[1]]!=list(df.columns):
            ws.delete_rows(1,ws.max_row)
            ws.append(list(df.columns))
            # Aplicar estilo cuando se recrea el header
            apply_table_style(ws, 1)
            print(f"[INFO] Header de hoja '{hoja}' recreado con estilo")
    
    # Obtener claves existentes para verificar duplicados (preferimos URL del mensaje de Slack)
    existing_keys = set()
    if ws.max_row > 1:  # Si hay datos además del header
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=True):
            if len(row) >= 3 and row[2]:  # SLACK column (índice 2)
                slack_content = str(row[2]).strip()
                url = extract_hyperlink_url(slack_content)
                existing_keys.add(url or slack_content)
    
    # Agregar solo mensajes nuevos
    new_rows_added = 0
    for _,r in df.iterrows():
        slack_content = str(r["SLACK"]).strip()
        if slack_content:
            key = extract_hyperlink_url(slack_content) or slack_content
            if key and key not in existing_keys:
                ws.append(list(r.values))
                existing_keys.add(key)
                new_rows_added += 1
    
    print(f"[INFO] Filas nuevas agregadas: {new_rows_added} (duplicados ignorados: {len(df) - new_rows_added})")
    
    # Aplicar estilo a la tabla siempre (incluso si no hay filas nuevas)
    if ws.max_row > 1:  # Si hay datos además del header
        apply_table_style(ws, ws.max_row)
        print(f"[INFO] Estilo aplicado a la tabla")
    
    if "Sheet" in wb.sheetnames and wb["Sheet"].max_row==1 and wb["Sheet"].max_column==1 and wb["Sheet"]["A1"].value is None:
        wb.remove(wb["Sheet"])

def main():
    print(f"[INFO] Inicio ejecución: {now_scl()}")
    token=acquire_token()
    print("[INFO] Access token obtenido")

    ensure_file(token)

    # Ejecutar siempre, sin restricción de hora
    print(f"[INFO] Ejecutando sin restricción de hora (debug_mode: {debug_mode})")

    # Últimos 4 días (en horario Chile): desde 00:00 del día (hoy - 3) hasta ahora
    now_local = now_scl()
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=3)
    oldest = str(start_local.astimezone(timezone.utc).timestamp())
    latest = str(datetime.now(tz=timezone.utc).timestamp())
    print(f"[INFO] Ventana Slack últimos 4 días (hora Chile): {start_local} hasta {now_local}")
    msgs = fetch_messages(oldest=oldest, latest=latest)

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
    
    # Intentar subir a OneDrive
    upload_success = up_excel(token,out)
    if upload_success:
        print(f"[INFO] Excel actualizado en OneDrive: {onedrive_file_path}")
    else:
        print(f"[WARN] No se pudo actualizar OneDrive, pero el procesamiento se completó exitosamente")

    print(f"[INFO] Fin ejecución: {now_scl()}")

if __name__=="__main__":
    main()
