"""
StorageIQ — CLIENT Scanner Agent (deployed into the CUSTOMER's Azure).

This is the half of the product that runs in the customer's own tenant. It
scans SharePoint (recursive, paginated, throttle-aware, resumable) and produces
RAW aggregate numbers — how much storage, how many versions, how much is cold.

It deliberately DOES NOT contain the savings / ranking / recommendation logic.
Instead it sends the anonymised aggregates to OUR Intelligence API and gets the
valuable output back. Without a valid licence key, the Intelligence API returns
nothing — so a copied scanner is useless on its own. That is the moat.

  Customer tenant (this agent)                Our server (Intelligence API)
  ─ scan SharePoint (raw numbers) ─►  send anonymised summary + licence ─►
  ◄──────────  savings + ranking + recommendations  ◄──────────

Data boundary: only aggregate numbers + site display names leave the tenant.
No file contents, no file paths, no user identities.
"""

import azure.functions as func
import azure.durable_functions as df
import datetime
import logging
import os
import time

import requests
from azure.identity import ClientSecretCredential

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

GRAPH = "https://graph.microsoft.com/v1.0"
COLD_DAYS = 90

# Where our Intelligence API lives + this customer's licence key.
# Set as app settings when the agent is deployed into the customer tenant.
INTELLIGENCE_API_URL = os.environ.get(
    "INTELLIGENCE_API_URL",
    "https://storageiq-intelligence.azurewebsites.net/api/intelligence")
LICENCE_KEY = os.environ.get("LICENCE_KEY", "DEMO-LICENCE")

MAX_RETRIES = 6
BACKOFF_BASE = 2.0
MAX_BACKOFF = 60.0


# ===========================================================================
# AUTH  (uses the CUSTOMER's own Entra app credentials, from their Key Vault)
# ===========================================================================
def _get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    tenant_id = tenant_id or os.environ.get("TENANT_ID")
    client_id = client_id or os.environ.get("CLIENT_ID")
    client_secret = client_secret or os.environ.get("CLIENT_SECRET")
    cred = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return cred.get_token("https://graph.microsoft.com/.default").token


# ===========================================================================
# GRAPH GET with throttling-aware retry + FULL pagination
# ===========================================================================
def graph_get(url: str, token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    attempt = 0
    while True:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 503, 504):
            attempt += 1
            if attempt > MAX_RETRIES:
                logging.warning("graph_get gave up after %s: %s",
                                MAX_RETRIES, url)
                return {}
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else BACKOFF_BASE * (2 ** (attempt - 1))
            except ValueError:
                wait = BACKOFF_BASE * (2 ** (attempt - 1))
            wait = min(wait, MAX_BACKOFF) + (0.1 * attempt)
            logging.info("Throttled %s; wait %.1fs (retry %s/%s)",
                         resp.status_code, wait, attempt, MAX_RETRIES)
            time.sleep(wait)
            continue
        logging.warning("graph_get %s -> %s", url, resp.status_code)
        return {}


def graph_get_all(url: str, token: str):
    while url:
        page = graph_get(url, token)
        if not page:
            return
        for item in page.get("value", []):
            yield item
        url = page.get("@odata.nextLink")


# ===========================================================================
# ACTIVITY: enumerate every site (paginated)
# ===========================================================================
@app.activity_trigger(input_name="creds")
def list_all_sites(creds: dict) -> list:
    token = _get_token(creds.get("tenant_id"), creds.get("client_id"),
                       creds.get("client_secret"))
    sites = []
    for s in graph_get_all(f"{GRAPH}/sites?search=*", token):
        sid = s.get("id")
        if not sid:
            continue
        sites.append({"id": sid,
                      "name": s.get("displayName") or s.get("name") or "Site"})
    logging.info("list_all_sites found %s sites", len(sites))
    return sites


# ===========================================================================
# ACTIVITY: fully scan ONE site (recursive, paginated, throttle-aware)
# Produces RAW numbers only — no savings maths here.
# ===========================================================================
@app.activity_trigger(input_name="job")
def scan_site(job: dict) -> dict:
    site = job["site"]
    creds = job["creds"]
    token = _get_token(creds.get("tenant_id"), creds.get("client_id"),
                       creds.get("client_secret"))
    site_id = site["id"]
    now = datetime.datetime.now(datetime.timezone.utc)

    s_used = s_version = s_cold = s_active = 0
    s_files = s_versions = 0

    for drive in graph_get_all(f"{GRAPH}/sites/{site_id}/drives", token):
        drive_id = drive.get("id")
        if not drive_id:
            continue
        stack = [f"{GRAPH}/drives/{drive_id}/root/children"]
        while stack:
            for item in graph_get_all(stack.pop(), token):
                if "folder" in item:
                    cid = item.get("id")
                    if cid:
                        stack.append(
                            f"{GRAPH}/drives/{drive_id}/items/{cid}/children")
                    continue
                if "file" not in item:
                    continue
                size = item.get("size", 0) or 0
                s_used += size
                s_files += 1
                lm = item.get("lastModifiedDateTime")
                cold = False
                if lm:
                    try:
                        dt = datetime.datetime.fromisoformat(
                            lm.replace("Z", "+00:00"))
                        cold = (now - dt).days > COLD_DAYS
                    except Exception:
                        cold = False
                if cold:
                    s_cold += size
                else:
                    s_active += size
                vlist = list(graph_get_all(
                    f"{GRAPH}/drives/{drive_id}/items/{item['id']}/versions",
                    token))
                if vlist:
                    s_versions += len(vlist)
                    if len(vlist) > 1:
                        older = sorted(
                            vlist,
                            key=lambda v: v.get("lastModifiedDateTime", ""),
                            reverse=True)[1:]
                        s_version += sum(v.get("size", 0) or 0 for v in older)

    gb = lambda b: round(b / (1024 ** 3), 4)
    return {
        "name": site["name"],
        "used_gb": gb(s_used),
        "version_gb": gb(s_version),
        "cold_gb": gb(s_cold),
        "active_gb": gb(s_active),
        "file_count": s_files,
        "version_count": s_versions,
        "avg_versions": round(s_versions / s_files, 1) if s_files else 0,
        "_raw": {"used": s_used, "version": s_version, "cold": s_cold,
                 "active": s_active, "files": s_files, "versions": s_versions},
    }


# ===========================================================================
# ACTIVITY: call OUR Intelligence API with the anonymised aggregates.
# (An activity, not inline in the orchestrator, because it does network I/O.)
# ===========================================================================
@app.activity_trigger(input_name="payload")
def call_intelligence(payload: dict) -> dict:
    try:
        r = requests.post(INTELLIGENCE_API_URL, json={
            "licence_key": LICENCE_KEY,
            "summary": payload.get("summary", {}),
            "sites": payload.get("sites", []),
        }, timeout=60)
        if r.status_code == 200:
            return r.json()
        return {"status": "error", "http": r.status_code,
                "message": r.text[:300]}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ===========================================================================
# ORCHESTRATOR: scan (fan-out) -> aggregate raw -> call Intelligence API
# ===========================================================================
@app.orchestration_trigger(context_name="context")
def scan_orchestrator(context: df.DurableOrchestrationContext):
    creds = context.get_input() or {}

    sites = yield context.call_activity("list_all_sites", creds)
    total = len(sites)

    results = []
    completed = 0
    BATCH = 12
    for i in range(0, total, BATCH):
        batch = sites[i:i + BATCH]
        tasks = [context.call_activity(
            "scan_site", {"site": s, "creds": creds}) for s in batch]
        results.extend((yield context.task_all(tasks)))
        completed += len(batch)
        context.set_custom_status({
            "phase": "scanning", "sites_total": total, "sites_done": completed})

    # ---- Aggregate RAW numbers (no savings maths here) ----------------
    raw = {"used": 0, "version": 0, "cold": 0, "active": 0,
           "files": 0, "versions": 0}
    site_rows = []
    for r in results:
        if not r:
            continue
        rr = r.get("_raw", {})
        for k in raw:
            raw[k] += rr.get(k, 0)
        r.pop("_raw", None)
        if r.get("file_count", 0) > 0:
            site_rows.append(r)

    gb = lambda b: round(b / (1024 ** 3), 4)
    summary = {
        "sites_scanned": len(site_rows),
        "sites_total": total,
        "total_used_gb": gb(raw["used"]),
        "version_storage_gb": gb(raw["version"]),
        "cold_storage_gb": gb(raw["cold"]),
        "active_storage_gb": gb(raw["active"]),
        "total_files": raw["files"],
        "total_versions": raw["versions"],
        "avg_versions_per_file": round(raw["versions"] / raw["files"], 1)
        if raw["files"] else 0,
        "generated_utc": context.current_utc_datetime.isoformat(),
    }

    # ---- Ask OUR Intelligence API for the valuable output -------------
    context.set_custom_status({
        "phase": "computing", "sites_total": total, "sites_done": total})
    intel = yield context.call_activity(
        "call_intelligence", {"summary": summary, "sites": site_rows})

    context.set_custom_status({
        "phase": "done", "sites_total": total, "sites_done": total})
    return {"status": "success", "summary": summary,
            "sites": site_rows, "intelligence": intel}


# ===========================================================================
# HTTP STARTER + STATUS
# ===========================================================================
@app.route(route="startscan", methods=["POST", "GET"])
@app.durable_client_input(client_name="client")
async def start_scan(req: func.HttpRequest, client) -> func.HttpResponse:
    body = {}
    try:
        body = req.get_json()
    except ValueError:
        body = {}

    def p(*names):
        for n in names:
            v = (req.params.get(n) or body.get(n) or "").strip()
            if v:
                return v
        return ""

    creds = {
        "tenant_id": p("tenant_id", "tenantId"),
        "client_id": p("client_id", "clientId"),
        "client_secret": p("client_secret", "clientSecret"),
    }
    instance_id = await client.start_new("scan_orchestrator", None, creds)
    logging.info("Started client scan %s", instance_id)
    return client.create_check_status_response(req, instance_id)


@app.route(route="scanstatus", methods=["GET"])
@app.durable_client_input(client_name="client")
async def scan_status(req: func.HttpRequest, client) -> func.HttpResponse:
    import json
    instance_id = req.params.get("id")
    if not instance_id:
        return func.HttpResponse('{"error":"pass ?id=<instanceId>"}',
                                 status_code=400, mimetype="application/json")
    status = await client.get_status(instance_id)
    out = {
        "instanceId": instance_id,
        "runtimeStatus": getattr(status, "runtime_status", None)
        and status.runtime_status.value,
        "customStatus": getattr(status, "custom_status", None),
        "output": getattr(status, "output", None),
    }
    return func.HttpResponse(json.dumps(out, default=str),
                             mimetype="application/json")
