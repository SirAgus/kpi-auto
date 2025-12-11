import os
import time
import json
import requests


def _post_form(url: str, data: dict) -> requests.Response:
    return requests.post(url, data=data, timeout=30)


def main() -> int:
    """
    Obtiene un refresh token para Microsoft Graph usando Device Code Flow.

    Requisitos (ENV):
      - AZURE_CLIENT_ID: Client ID de tu App Registration
      - AZURE_TENANT: (opcional) consumers | common | <tenant-id>. Default: consumers
      - GRAPH_SCOPE: (opcional) scopes separados por espacio. Default: 'offline_access Files.ReadWrite'
      - AZURE_CLIENT_SECRET: (opcional) si tu app es confidencial (normalmente NO para este proyecto)
    """
    client_id = os.environ.get("AZURE_CLIENT_ID", "").strip()
    if not client_id:
        print("[ERROR] Falta AZURE_CLIENT_ID en variables de entorno.")
        return 2

    tenant = os.environ.get("AZURE_TENANT", "consumers").strip() or "consumers"
    scope = os.environ.get("GRAPH_SCOPE", "offline_access Files.ReadWrite").strip()
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "").strip()

    device_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/devicecode"
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

    dc_resp = _post_form(device_url, {"client_id": client_id, "scope": scope})
    if dc_resp.status_code >= 400:
        print(f"[ERROR] Device code falló (status={dc_resp.status_code}): {dc_resp.text}")
        return 1

    dc = dc_resp.json()
    verification_uri = dc.get("verification_uri") or dc.get("verification_url")
    user_code = dc.get("user_code")
    message = dc.get("message")
    interval = int(dc.get("interval", 5))
    expires_in = int(dc.get("expires_in", 900))
    device_code = dc.get("device_code")

    print("\n=== AUTORIZACIÓN ===")
    if message:
        print(message)
    else:
        print(f"1) Abrí: {verification_uri}")
        print(f"2) Ingresá el código: {user_code}")

    print("\n=== ESPERANDO AUTORIZACIÓN (no cierres esto) ===")
    deadline = time.time() + expires_in

    while time.time() < deadline:
        time.sleep(interval)

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "client_id": client_id,
            "device_code": device_code,
        }
        if client_secret:
            data["client_secret"] = client_secret

        tok_resp = _post_form(token_url, data)
        if tok_resp.status_code == 200:
            tok = tok_resp.json()
            refresh = tok.get("refresh_token")

            print("\n=== RESULTADO ===")
            if not refresh:
                print("[WARN] No vino refresh_token en la respuesta.")
                print("Asegurate de incluir 'offline_access' en GRAPH_SCOPE y que tu app permita flows de cliente público.")
                print("Respuesta completa (recortada):")
                print(json.dumps({k: tok.get(k) for k in tok.keys() if k != "access_token"}, indent=2)[:2000])
                return 1

            print("Copiá este valor y setéalo como GRAPH_REFRESH_TOKEN:\n")
            print(refresh)
            return 0

        # Manejo de estados normales del device flow
        try:
            err = tok_resp.json()
        except ValueError:
            print(f"[ERROR] Respuesta no-JSON (status={tok_resp.status_code}): {tok_resp.text}")
            return 1

        code = err.get("error")
        desc = err.get("error_description", "")

        if code == "authorization_pending":
            continue
        if code == "slow_down":
            interval = interval + 5
            continue

        print(f"[ERROR] Token falló: {code}: {desc}")
        return 1

    print("[ERROR] Expiró el device_code. Volvé a correr el script.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


