import os, io, requests, pandas as pd
from datetime import timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import msal
import re
import time

try:
    from groq import Groq
except ImportError:
    Groq = None

slack_bot_token=os.environ["SLACK_BOT_TOKEN"]
channel_id=os.environ["SLACK_CHANNEL_ID"]
client_id=os.environ["AZURE_CLIENT_ID"]
refresh_token=os.environ.get("GRAPH_REFRESH_TOKEN","")
onedrive_upn=os.environ["ONEDRIVE_UPN"]
requested_onedrive_file_path=(os.environ.get("ONEDRIVE_FILE_PATH","/Documents/BlackBox.xlsx").strip() or "/Documents/BlackBox.xlsx")
debug_mode=os.environ.get("DEBUG_MODE","0")=="1"
run_mode=(os.environ.get("RUN_MODE","").strip().lower() or ("dev" if debug_mode else "prod"))

def build_dev_onedrive_path(path):
    p=(path or "").strip() or "/Documents/BlackBox.xlsx"
    if p.lower().endswith("_dev.xlsx"):
        return p
    if p.lower().endswith(".xlsx"):
        return p[:-5] + "_dev.xlsx"
    return p + "_dev.xlsx"

onedrive_file_path=build_dev_onedrive_path(requested_onedrive_file_path) if run_mode=="dev" else requested_onedrive_file_path
# target_hour_local eliminado - ya no se usa restricción de hora
dev_team_member_ids=[i.strip() for i in os.environ.get("DEV_TEAM_MEMBER_IDS","").split(",") if i.strip()]
refresh_token_path=os.environ.get("REFRESH_TOKEN_PATH","/data/graph_refresh_token")
device_flow_wait_seconds=int(os.environ.get("DEVICE_FLOW_WAIT_SECONDS","600"))  # 10 min
graph_scope=os.environ.get("GRAPH_SCOPE","offline_access Files.ReadWrite").strip() or "offline_access Files.ReadWrite"
groq_api_key=os.environ.get("GROQ_API_KEY","").strip()
groq_model=os.environ.get("GROQ_MODEL","qwen/qwen3-32b").strip() or "qwen/qwen3-32b"

expected_columns=[
    "Fecha aproximada",
    "Origen",
    "SLACK",
    "Funcionalidad Backend/Frontend",
    "Comentarios",
    "Categoría Soporte (estandarizado para reportes)",
    "Propuesta (Tarea en ClickUp cuando sea desarrollable /Cambio sistema)",
    "ESTADO FINAL",
    "resumen ia"
]
legacy_diagnosis_column="Diagnóstico causa raíz"
hyperlink_formula_pattern=re.compile(r'^=HYPERLINK\("([^"]*)","((?:[^"]|"")*)"\)$')
user_display_names={
    "U06BJ8JQ7B8":"agustin",
    "U07UBRSER6D":"neil",
    "U05D1H8JPEJ":"vico",
    "U06BF8NPZ5J":"luna"
}

def replace_known_user_ids(text):
    out=str(text or "")
    for user_id, display_name in user_display_names.items():
        out=out.replace(f"<@{user_id}>", display_name)
        out=re.sub(rf"\b{re.escape(user_id)}\b", display_name, out)
    return out

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

def sanitize_text(text, max_len=None, escape_quotes=False):
    if text is None:
        return ""
    out=replace_known_user_ids(text).replace("\n"," ").replace("\r"," ")
    out=re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"," ",out)
    out=re.sub(r"\s+"," ",out).strip()
    if max_len and len(out)>max_len:
        out=out[:max_len-3]+"..."
    if escape_quotes:
        out=out.replace('"','""')
    return out

def build_hyperlink_formula(url, label):
    safe_label=sanitize_text(label, max_len=200, escape_quotes=True)
    if not safe_label:
        safe_label="Abrir mensaje"
    formula=f'=HYPERLINK("{url}","{safe_label}")'
    if not hyperlink_formula_pattern.match(formula):
        formula=f'=HYPERLINK("{url}","Abrir mensaje")'
    return formula

def build_slack_hyperlink(ts, text):
    ts_formatted=str(ts or "").replace(".","")
    slack_link=f"https://mq-sede.slack.com/archives/{channel_id}/p{ts_formatted}"
    return build_hyperlink_formula(slack_link, text), slack_link

def fetch_thread_replies(client, thread_ts):
    if not thread_ts:
        return []
    out=[]
    cur=None
    while True:
        try:
            res=client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                limit=200,
                cursor=cur
            )
        except SlackApiError as e:
            err=e.response.get("error") if getattr(e, "response", None) else str(e)
            print(f"[WARN] No se pudo leer hilo {thread_ts}: {err}")
            break
        out.extend(res.get("messages",[]))
        cur=res.get("response_metadata",{}).get("next_cursor")
        if not cur:
            break
    return out

def build_comments_from_thread(replies, root_ts):
    comentarios=[]
    for msg in replies:
        if msg.get("ts")==root_ts:
            continue
        txt=sanitize_text(msg.get("text",""), max_len=600)
        if not txt:
            continue
        user_id=msg.get("user")
        author=user_display_names.get(user_id, user_id) if user_id else (msg.get("bot_id") or "desconocido")
        ts=msg.get("ts")
        if ts:
            fecha=tz_dt(ts).strftime("%Y-%m-%d %H:%M:%S")
            comentarios.append(f"[{fecha}] {author}: {txt}")
        else:
            comentarios.append(f"{author}: {txt}")
    merged=" | ".join(comentarios).strip()
    return merged[:3997]+"..." if len(merged)>4000 else merged

def create_groq_client():
    if not groq_api_key:
        print("[WARN] GROQ_API_KEY no configurada; 'resumen ia' se completará con fallback.")
        return None
    if Groq is None:
        print("[WARN] Librería groq no instalada; 'resumen ia' se completará con fallback.")
        return None
    try:
        return Groq(api_key=groq_api_key)
    except Exception as e:
        print(f"[WARN] No se pudo crear cliente Groq: {e}")
        return None

def generate_ai_summary(groq_client, main_text, comments_text):
    if not comments_text:
        return "Sin comentarios en el hilo. Conclusión: Sin información suficiente."
    if groq_client is None:
        return "No se pudo generar resumen IA (sin cliente Groq). Conclusión: Sin información suficiente."

    prompt=(
        "Analiza este hilo de soporte de Slack y entrega un resumen breve.\n"
        "Mensaje principal:\n"
        f"{sanitize_text(main_text, max_len=2000)}\n\n"
        "Comentarios del hilo:\n"
        f"{sanitize_text(comments_text, max_len=6000)}\n\n"
        "Responde en español con máximo 3 frases. "
        "No incluyas razonamiento interno, ni etiquetas XML/HTML como <think>. "
        "La última frase debe empezar exactamente con 'Conclusión:' y usar solo una etiqueta: "
        "'Resuelto', 'No resuelto' o 'Sin información suficiente'."
    )

    try:
        completion=groq_client.chat.completions.create(
            model=groq_model,
            messages=[
                {
                    "role":"user",
                    "content":prompt
                }
            ],
            temperature=0.6,
            max_completion_tokens=4096,
            top_p=0.95,
            reasoning_effort="default",
            stream=True,
            stop=None
        )
        output=[]
        for chunk in completion:
            output.append(chunk.choices[0].delta.content or "")
        summary=normalize_ai_summary("".join(output))
        if summary:
            return summary[:4997]+"..." if len(summary)>5000 else summary
    except Exception as e:
        print(f"[WARN] Error generando resumen IA: {e}")

    return "No se pudo generar resumen IA. Conclusión: Sin información suficiente."

def normalize_ai_summary(text):
    s=(text or "").strip()
    s=re.sub(r"<think>.*?</think>\s*","",s,flags=re.IGNORECASE|re.DOTALL)
    s=s.replace("**","").replace("__","")
    s=sanitize_text(s)
    return s

def infer_final_status(summary):
    s=normalize_ai_summary(summary).lower()
    s=re.sub(r"[^a-záéíóúüñ ]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    if "conclusión resuelto" in s or "conclusion resuelto" in s:
        return "Resuelto"
    if "conclusión no resuelto" in s or "conclusion no resuelto" in s:
        return "No resuelto"
    if "conclusión sin información suficiente" in s or "conclusion sin información suficiente" in s:
        return "Sin información suficiente"
    return ""

def build_df(msgs, existing_keys=None):
    datos=[]
    known_keys=set(existing_keys or [])
    slack_client=WebClient(token=slack_bot_token)
    groq_client=create_groq_client()

    for m in reversed(msgs):
        uid=m.get("user")
        if not uid:
            continue

        ts=m.get("ts")
        if not ts:
            continue

        slack_content, slack_link=build_slack_hyperlink(ts, m.get("text",""))
        key=slack_link or slack_content
        if key in known_keys:
            continue

        dt=tz_dt(ts)
        origen="Producto" if uid in dev_team_member_ids else "Otras áreas"

        comments_for_ai=""
        reply_count=int(m.get("reply_count",0) or 0)
        if reply_count>0:
            thread_ts=m.get("thread_ts") or ts
            replies=fetch_thread_replies(slack_client, thread_ts)
            comments_for_ai=build_comments_from_thread(replies, ts)

        resumen_ia=generate_ai_summary(groq_client, m.get("text",""), comments_for_ai)

        datos.append({
            "Fecha aproximada":dt.strftime("%Y-%m-%d %H:%M:%S"),
            "Origen":origen,
            "SLACK":slack_content,
            "Funcionalidad Backend/Frontend":"",
            "Comentarios":"",
            "Categoría Soporte (estandarizado para reportes)":"",
            "Propuesta (Tarea en ClickUp cuando sea desarrollable /Cambio sistema)":"",
            "ESTADO FINAL":"",
            "resumen ia":resumen_ia
        })
        known_keys.add(key)

    return pd.DataFrame(datos,columns=expected_columns) if datos else pd.DataFrame(columns=expected_columns)

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
        for col in range(1, ws.max_column + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border
        
        # Aplicar estilo a las filas de datos
        for row in range(2, min(num_rows + 1, ws.max_row + 1)):
            for col in range(1, ws.max_column + 1):
                cell = ws.cell(row=row, column=col)
                cell.font = data_font
                cell.alignment = data_alignment
                cell.border = thin_border
        
        # Ajustar ancho de columnas
        column_widths = {
            1: 23,  # Fecha aproximada
            2: 18,  # Origen
            3: 60,  # SLACK
            4: 30,  # Funcionalidad Backend/Frontend
            5: 65,  # Comentarios
            6: 42,  # Categoría Soporte
            7: 48,  # Propuesta
            8: 22,  # ESTADO FINAL
            9: 70   # resumen ia
        }
        
        for col_index, width in column_widths.items():
            col_letter = get_column_letter(col_index)
            ws.column_dimensions[col_letter].width = width
        
        # Ajustar altura de filas
        max_rows = min(num_rows, 200)
        for row in range(1, max_rows + 1):
            ws.row_dimensions[row].height = 42
        
        # Asegurar que el zoom esté al 100%
        ws.sheet_view.zoomScale = 100
        
        print(f"[INFO] Estilo aplicado: {max_rows} filas, columnas ajustadas")
            
    except Exception as e:
        print(f"[WARN] Error aplicando estilo: {e}")
        # Continuar sin estilo si hay problemas

def collect_existing_slack_keys(wb):
    keys=set()
    for sheet_name in wb.sheetnames:
        ws=wb[sheet_name]
        if ws.max_row<=1:
            continue
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=True):
            if len(row)>=3 and row[2]:
                slack_content=str(row[2]).strip()
                url=extract_hyperlink_url(slack_content)
                keys.add(url or slack_content)
    return keys

def collect_existing_row_locations(wb):
    locations={}
    for sheet_name in wb.sheetnames:
        ws=wb[sheet_name]
        if ws.max_row<=1:
            continue
        for row_idx in range(2, ws.max_row+1):
            slack_value=ws.cell(row=row_idx, column=3).value
            if not slack_value:
                continue
            slack_content=str(slack_value).strip()
            key=extract_hyperlink_url(slack_content) or slack_content
            if key and key not in locations:
                locations[key]=(sheet_name, row_idx)
    return locations

def backfill_ai_for_existing_rows(wb, msgs, existing_row_locations):
    if not msgs or not existing_row_locations:
        return 0, 0

    slack_client=WebClient(token=slack_bot_token)
    groq_client=create_groq_client()
    checked=0
    updated=0

    for m in reversed(msgs):
        uid=m.get("user")
        ts=m.get("ts")
        if not uid or not ts:
            continue

        _, slack_link=build_slack_hyperlink(ts, m.get("text",""))
        key=slack_link
        if key not in existing_row_locations:
            continue

        sheet_name, row_idx = existing_row_locations[key]
        ws=wb[sheet_name]
        status_cell=ws.cell(row=row_idx, column=8)
        summary_cell=ws.cell(row=row_idx, column=9)
        has_status=bool(str(status_cell.value or "").strip())
        has_summary=bool(str(summary_cell.value or "").strip())

        if has_status and has_summary:
            continue

        comments_for_ai=""
        reply_count=int(m.get("reply_count",0) or 0)
        if reply_count>0:
            thread_ts=m.get("thread_ts") or ts
            replies=fetch_thread_replies(slack_client, thread_ts)
            comments_for_ai=build_comments_from_thread(replies, ts)

        new_summary=normalize_ai_summary(generate_ai_summary(groq_client, m.get("text",""), comments_for_ai))
        checked+=1
        if new_summary and new_summary != str(summary_cell.value or "").strip():
            summary_cell.value=new_summary
            updated+=1

    if checked:
        print(f"[INFO] Backfill IA sobre filas existentes: revisadas={checked}, actualizadas={updated}")
    return checked, updated

def normalize_header_row(ws):
    return [str(ws.cell(row=1, column=idx).value).strip() if ws.cell(row=1, column=idx).value is not None else "" for idx in range(1, ws.max_column + 1)]

def migrate_sheet_headers(ws):
    current_headers=normalize_header_row(ws)
    if current_headers==expected_columns:
        return False

    header_index={name: idx for idx, name in enumerate(current_headers) if name}
    existing_rows=list(ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=True)) if ws.max_row>1 else []
    remapped=[]
    for old_row in existing_rows:
        new_row=[]
        for col_name in expected_columns:
            value=""
            idx=header_index.get(col_name)
            if idx is not None and idx < len(old_row):
                value=old_row[idx]
            elif col_name=="Comentarios":
                legacy_idx=header_index.get(legacy_diagnosis_column)
                if legacy_idx is not None and legacy_idx < len(old_row):
                    value=old_row[legacy_idx]
            new_row.append("" if value is None else value)
        remapped.append(new_row)

    ws.delete_rows(1, ws.max_row)
    ws.append(expected_columns)
    for row in remapped:
        ws.append(row)
    return True

def repair_invalid_hyperlinks_in_sheet(ws):
    repaired=0
    if ws.max_row<=1:
        return repaired
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=3, max_col=3):
        cell=row[0]
        val=cell.value
        if not isinstance(val,str) or not val.startswith("=HYPERLINK("):
            continue
        if hyperlink_formula_pattern.match(val):
            continue
        url=extract_hyperlink_url(val)
        if url:
            cell.value=build_hyperlink_formula(url, "Abrir mensaje")
            repaired+=1
    return repaired

def repair_ai_columns_in_sheet(ws):
    cleaned=0
    if ws.max_row<=1:
        return cleaned
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        summary_cell=row[8] if len(row)>=9 else None
        if summary_cell is not None and isinstance(summary_cell.value, str) and summary_cell.value.strip():
            normalized=normalize_ai_summary(summary_cell.value)
            if normalized and normalized != summary_cell.value:
                summary_cell.value=normalized
                cleaned+=1
    return cleaned

def migrate_and_repair_workbook(wb):
    migrated=0
    repaired=0
    ai_cleaned=0
    for sheet_name in wb.sheetnames:
        ws=wb[sheet_name]
        # No tocar hojas vacías temporales
        if ws.max_row==1 and ws.max_column==1 and ws["A1"].value is None:
            continue
        if migrate_sheet_headers(ws):
            migrated+=1
        repaired+=repair_invalid_hyperlinks_in_sheet(ws)
        ai_cleaned += repair_ai_columns_in_sheet(ws)
        if ws.max_row>1:
            apply_table_style(ws, ws.max_row)
    if migrated or repaired or ai_cleaned:
        print(
            "[INFO] Migración/Reparación aplicada - "
            f"hojas migradas: {migrated}, hipervínculos reparados: {repaired}, "
            f"resúmenes IA limpiados: {ai_cleaned}"
        )
    return migrated, repaired, ai_cleaned

def append_rows(wb,df):
    if df.empty:
        return
    
    hoja = get_month_name_from_period(df)
    
    # Verificar si la hoja ya existe
    if hoja not in wb.sheetnames:
        ws=wb.create_sheet(title=hoja)
        ws.append(expected_columns)
        # Aplicar estilo inmediatamente al crear nueva hoja
        apply_table_style(ws, 1)
        print(f"[INFO] Nueva hoja '{hoja}' creada con estilo")
    else:
        ws=wb[hoja]
        if migrate_sheet_headers(ws):
            apply_table_style(ws, 1)
            print(f"[INFO] Hoja '{hoja}' migrada al nuevo esquema de columnas")
    
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
                ws.append([r.get(col, "") for col in expected_columns])
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
    print(f"[INFO] Modo ejecución: {run_mode} | OneDrive destino: {onedrive_file_path}")
    token=acquire_token()
    print("[INFO] Access token obtenido")

    ensure_file(token)

    bio=dl_excel(token)
    try:
        wb=load_workbook(bio)
        print("[INFO] Excel cargado")
    except Exception:
        wb=Workbook()
        wb.active.title="TMP"
        print("[WARN] Excel nuevo creado")

    migrated_sheets, repaired_links, ai_cleaned = migrate_and_repair_workbook(wb)
    existing_slack_keys=collect_existing_slack_keys(wb)
    existing_row_locations=collect_existing_row_locations(wb)
    print(f"[INFO] Claves Slack ya existentes en Excel: {len(existing_slack_keys)}")

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
    _, backfilled_ai = backfill_ai_for_existing_rows(wb, msgs, existing_row_locations)
    df=build_df(msgs, existing_keys=existing_slack_keys)
    workbook_changed = (migrated_sheets > 0 or repaired_links > 0 or ai_cleaned > 0 or backfilled_ai > 0)
    if not df.empty:
        print(f"[INFO] Filas a agregar: {len(df)}")
        print("[DEBUG] Preview:\n", df.head(5).to_string())
        append_rows(wb,df)
        sheet_name = get_month_name_from_period(df)
        print(f"[INFO] Datos procesados en hoja '{sheet_name}'")
        workbook_changed = True
    else:
        print("[INFO] No hay mensajes nuevos")
        if not workbook_changed:
            return

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
