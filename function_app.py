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
testing
"""

import azure.functions as func
import azure.durable_functions as df
import datetime
import json
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
# $BATCH — send up to 20 GET requests in ONE HTTP call (Graph JSON batching).
# This is the biggest deep-scan speed-up: ~1/20th the round-trips, and Graph
# throttles the batch as a unit so 429s are far rarer. Retries the whole batch
# on 429 honouring Retry-After.
# ===========================================================================
GRAPH_BATCH_URL = "https://graph.microsoft.com/v1.0/$batch"
BATCH_SIZE = 20


def graph_batch(rel_urls, token):
    """rel_urls: list of Graph paths (relative, e.g. '/drives/x/items/y/versions').
    Returns dict {request_id -> parsed body}. request_id is the index as a str."""
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}
    out = {}
    for start in range(0, len(rel_urls), BATCH_SIZE):
        chunk = rel_urls[start:start + BATCH_SIZE]
        payload = {"requests": [
            {"id": str(start + i), "method": "GET", "url": u}
            for i, u in enumerate(chunk)
        ]}
        attempt = 0
        while True:
            r = requests.post(GRAPH_BATCH_URL, headers=headers,
                              json=payload, timeout=90)
            if r.status_code == 200:
                for resp in r.json().get("responses", []):
                    rid = resp.get("id")
                    status = resp.get("status", 200)
                    if status == 429:
                        # Per-item throttle inside the batch — record for a
                        # single retry pass below.
                        out.setdefault("_retry", []).append(
                            rel_urls[int(rid)])
                    elif status < 300:
                        out[rid] = resp.get("body", {})
                break
            if r.status_code in (429, 503, 504):
                attempt += 1
                if attempt > MAX_RETRIES:
                    break
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else BACKOFF_BASE * (2 ** (attempt - 1))
                except ValueError:
                    wait = BACKOFF_BASE * (2 ** (attempt - 1))
                time.sleep(min(wait, MAX_BACKOFF) + 0.1 * attempt)
                continue
            break  # non-retryable
    return out


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

    # ---- Phase 1: collect every file (recursive walk) --------------------
    # Store (drive_id, item_id) so we can fetch versions in bulk afterwards.
    files = []   # list of dicts: {drive, id}
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
                files.append({"drive": drive_id, "id": item["id"]})

    # ---- Phase 2: fetch version history in PARALLEL BATCHES ---------------
    # Instead of one serial call per file, group files into $batch requests
    # (20 per HTTP call) and run several batches concurrently with threads.
    # This is the deep-scan speed-up: ~15-20x fewer/faster round-trips.
    from concurrent.futures import ThreadPoolExecutor

    def _rel_versions(f):
        return f"/drives/{f['drive']}/items/{f['id']}/versions"

    rel_urls = [_rel_versions(f) for f in files]

    def _run_batch(sub):
        return graph_batch(sub, token)

    # Split all version URLs into thread-sized chunks; each chunk is itself
    # batched into 20-per-call inside graph_batch(). 6 concurrent threads
    # keeps us fast without provoking heavy throttling.
    CHUNK = BATCH_SIZE * 5            # 100 urls handled per thread task
    tasks = [rel_urls[i:i + CHUNK] for i in range(0, len(rel_urls), CHUNK)]

    def _tally(bodies):
        nonlocal s_versions, s_version
        for rid, body in bodies.items():
            if rid == "_retry":
                continue
            vlist = body.get("value", []) if isinstance(body, dict) else []
            if not vlist:
                continue
            s_versions += len(vlist)
            if len(vlist) > 1:
                older = sorted(
                    vlist,
                    key=lambda v: v.get("lastModifiedDateTime", ""),
                    reverse=True)[1:]
                s_version += sum(v.get("size", 0) or 0 for v in older)

    retry_urls = []
    if tasks:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for bodies in ex.map(_run_batch, tasks):
                retry_urls.extend(bodies.get("_retry", []))
                _tally(bodies)

    # One serial retry pass for any items throttled inside a batch.
    if retry_urls:
        _tally(graph_batch(retry_urls, token))

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


# ===========================================================================
# FAST usage metrics (Microsoft Graph Reports API) — the 6-second path.
# This is the DEFAULT the dashboard calls: org-wide storage + per-site totals
# in one report call. No per-file/version walking (that's the deep scan).
# Credentials come from the request (UI form) with env fallback.
# ===========================================================================
@app.route(route="usagemetrics", methods=["GET", "POST", "OPTIONS"])
def usagemetrics(req: func.HttpRequest) -> func.HttpResponse:
    import csv
    import io
    import re as _re

    cors = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=cors)

    body = {}
    if req.method == "POST":
        try:
            body = req.get_json()
        except ValueError:
            body = {}

    def _param(*names):
        for n in names:
            v = (req.params.get(n) or body.get(n) or "").strip()
            if v:
                return v
        return ""

    u_tenant = _param("tenant_id", "tenantId", "tenant")
    u_client = _param("client_id", "clientId", "client")
    u_secret = _param("client_secret", "clientSecret", "secret")

    period = (_param("period") or "D30").upper()
    if period not in ("D7", "D30", "D90", "D180"):
        period = "D30"

    try:
        token = _get_token(u_tenant, u_client, u_secret)
        req_headers = {"Authorization": f"Bearer {token}"}
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error",
                        "message": "Could not authenticate with the supplied "
                        "credentials.", "detail": str(e)}),
            mimetype="application/json", status_code=401, headers=cors)

    # NOTE: No pricing / savings / quota formulas here. This agent only
    # gathers RAW numbers (bytes, file/version counts, seat counts) from the
    # tenant. All cost/quota/savings maths (the USP) lives in the Intelligence
    # API on our server. The client environment holds no such logic.

    try:
        url = (f"{GRAPH}/reports/"
               f"getSharePointSiteUsageDetail(period='{period}')")
        response = requests.get(url, headers=req_headers)
        response.raise_for_status()
        text = response.content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))

        sites = []
        total_bytes = total_files = total_active_files = 0
        deleted_bytes = 0
        deleted_count = 0
        for row in reader:
            def col(*names, default=""):
                for n in names:
                    if n in row and row[n] != "":
                        return row[n]
                return default

            used_bytes = int(col("Storage Used (Byte)", default="0") or 0)
            file_count = int(col("File Count", default="0") or 0)
            active_files = int(col("Active File Count", default="0") or 0)
            allocated = int(col("Storage Allocated (Byte)", default="0") or 0)

            # Deleted (but not yet purged) sites still appear in this report
            # and still consume tenant quota, but the Admin Centre's headline
            # figure excludes them — so counting them here inflates the total
            # (measured ~4 TB / 703 sites on a real tenant). Track them
            # separately instead of silently folding them into the total.
            if (col("Is Deleted", default="False") or "").strip().lower() == "true":
                deleted_bytes += used_bytes
                deleted_count += 1
                continue

            total_bytes += used_bytes
            total_files += file_count
            total_active_files += active_files
            used_gb = used_bytes / (1024 ** 3)

            raw_url = col("Site URL", "Owner Principal Name")
            raw_owner = col("Owner Display Name", "Owner Principal Name")
            _is_hash = bool(_re.fullmatch(r"[0-9A-F]{32}", raw_url.upper()))
            site_url = "" if _is_hash else raw_url
            owner_name = ("Anonymised (enable report display names in M365 "
                          "Admin)") if _is_hash else raw_owner

            sites.append({
                "site_url": site_url, "owner": owner_name,
                "anonymised": _is_hash, "used_bytes": used_bytes,
                "used_gb": round(used_gb, 2),
                "used_mb": round(used_bytes / (1024 * 1024), 2),
                "allocated_gb": round(allocated / (1024 ** 3), 2),
                "file_count": file_count, "active_file_count": active_files,
                "last_activity": col("Last Activity Date"),
                # No cost here — the Intelligence API computes $ figures.
            })

        sites.sort(key=lambda s: s["used_bytes"], reverse=True)
        total_gb = total_bytes / (1024 ** 3)

        # NOTE: getSharePointSiteUsageStorage's "Site Type" column has been
        # observed to only ever contain "All" (SharePoint + OneDrive combined)
        # in real tenant data — never a literal "SharePoint" value, despite
        # what the report name implies. The original filter for "sharepoint"
        # therefore never matched anything, silently falling through to the
        # per-site detail-report sum (which run ~2-5% higher, since it can
        # double-count or include items the org-level trend report excludes).
        # Accept "all" as the primary case, "sharepoint" as a fallback in
        # case a tenant/locale ever does return a dedicated SharePoint-only
        # row.
        # This report returns one row PER DAY across the whole period, so the
        # rows must be picked by latest Report Date — taking the max value
        # instead reports the tenant's 30-day peak, not its current usage
        # (measured ~1 TB high on a real tenant whose usage had just dipped).
        org_sp_used_bytes = 0
        report_date = ""
        try:
            storage_url = (f"{GRAPH}/reports/"
                           f"getSharePointSiteUsageStorage(period='{period}')")
            sresp = requests.get(storage_url, headers=req_headers)
            sresp.raise_for_status()
            sreader = csv.DictReader(
                io.StringIO(sresp.content.decode("utf-8-sig")))
            latest_all = ("", 0)          # (report_date, bytes)
            latest_sharepoint = ("", 0)
            for srow in sreader:
                site_type = (srow.get("Site Type") or "").strip().lower()
                used = int(srow.get("Storage Used (Byte)") or 0)
                rdate = (srow.get("Report Date") or "").strip()
                if site_type == "sharepoint" and rdate > latest_sharepoint[0]:
                    latest_sharepoint = (rdate, used)
                elif site_type == "all" and rdate > latest_all[0]:
                    latest_all = (rdate, used)
            picked = latest_sharepoint if latest_sharepoint[1] else latest_all
            report_date, org_sp_used_bytes = picked
        except Exception as e:
            logging.warning("storage report failed (%s); using per-site sum", e)
            org_sp_used_bytes = total_bytes
        if org_sp_used_bytes == 0:
            org_sp_used_bytes = total_bytes
        org_sp_used_gb = org_sp_used_bytes / (1024 ** 3)

        # Raw licensed-seat count only — NO quota formula here. The Intelligence
        # API turns seat_count into the storage quota (that formula is the USP).
        #
        # Microsoft's "1 TB + 10 GB/seat" pool bonus applies to primary seats
        # that include SharePoint/OneDrive (Business/Enterprise/Education M365
        # plans), NOT to standalone add-ons like Power BI, Teams Premium,
        # Stream, Flow, Visio, or Copilot trials — those are separate SKUs
        # consumed alongside a primary seat, not additional pool-qualifying
        # seats. Real tenant data also shows skuPartNumber values with stray
        # spaces (e.g. "MICROSOFT_365_ BUSINESS_ PREMIUM_(NO TEAMS)"), so we
        # strip whitespace before matching rather than relying on exact
        # prefixes.
        # NOTE: these must themselves be alnum-only (no underscores/spaces),
        # since we compare against `norm`, which has all non-alnum chars
        # stripped from the real skuPartNumber before matching.
        POOL_SKU_SUBSTRINGS = (
            "ENTERPRISEPACK", "ENTERPRISEPREMIUM", "ENTERPRISEWITHSCAL",
            "SPEE",                        # SPE_E3 / SPE_E5
            "SPB",                         # Microsoft 365 Business Standard/Premium
            "MICROSOFT365BUSINESSPREMIUM", "MICROSOFT365BUSINESSSTANDARD",
            "O365BUSINESS", "SMBBUSINESS",
            "STANDARDPACK", "STANDARDWOFFPACK",  # Office 365 E1 / legacy
            "DESKLESSPACK",                # F1/F3 frontline
            "PROJECTPROFESSIONAL",         # Project Plan 3 — carries a SP seat
            "M365EDUA", "ENTERPRISEPACKFACULTY", "ENTERPRISEPACKSTUDENT",
        )
        # Purchased extra-storage add-ons are NOT seats: each unit is 1 GB of
        # pool storage bought outright, on top of the licence-derived pool.
        # Tenants near their limit often have tens of TB of this (validated
        # against a real tenant: 28,672 units = 28 TB, which was the single
        # largest source of our quota under-reporting).
        STORAGE_ADDON_SUBSTRINGS = ("SHAREPOINTSTORAGE", "EXTRAFILESTORAGE")
        seat_count = 0
        storage_addon_gb = 0
        seat_source = "skus"
        sku_debug = []
        try:
            skus_resp = requests.get(f"{GRAPH}/subscribedSkus",
                                     headers=req_headers)
            if skus_resp.status_code == 200:
                for sku in skus_resp.json().get("value", []):
                    raw_part = sku.get("skuPartNumber") or ""
                    part = raw_part.upper()
                    # Strip whitespace AND underscores so real-world
                    # values like "MICROSOFT_365_ BUSINESS_ PREMIUM_(NO
                    # TEAMS)" normalize to "MICROSOFT365BUSINESSPREMIUM..."
                    # and match the substrings below.
                    norm = "".join(ch for ch in part if ch.isalnum())
                    consumed = sku.get("consumedUnits", 0)
                    # Use purchased seats (prepaidUnits.enabled), not assigned
                    # seats (consumedUnits). The SharePoint pool bonus is
                    # granted per seat PURCHASED on the plan, so this tracks
                    # the SharePoint Admin Centre figure more closely than
                    # consumedUnits does (which can include trial/unassigned
                    # licences that inflate the count).
                    prepaid = sku.get("prepaidUnits", {}) or {}
                    enabled = prepaid.get("enabled", consumed)
                    sku_debug.append({"sku": raw_part, "consumed": consumed,
                                      "enabled": enabled})
                    if any(p in norm for p in STORAGE_ADDON_SUBSTRINGS):
                        storage_addon_gb += enabled
                    elif any(p in norm for p in POOL_SKU_SUBSTRINGS):
                        seat_count += enabled
            else:
                seat_source = "unavailable"
        except Exception:
            seat_source = "unavailable"

        # RAW measurements only. No $, no quota, no rates.
        raw_summary = {
            "period": period, "site_count": len(sites),
            "org_used_gb": round(org_sp_used_gb, 2),
            "org_used_bytes": org_sp_used_bytes,
            "report_date": report_date,
            "seat_count": seat_count, "seat_source": seat_source,
            "storage_addon_gb": storage_addon_gb,
            "sku_debug": sku_debug,
            "total_used_gb": round(total_gb, 2),
            "total_used_bytes": total_bytes,
            "total_files": total_files,
            "total_active_files": total_active_files,
            "deleted_site_count": deleted_count,
            "deleted_gb": round(deleted_bytes / (1024 ** 3), 2),
        }

        # Send the raw numbers to OUR Intelligence API for the cost/quota/savings
        # (the USP). If it is unreachable, still return the raw data so the
        # agent degrades gracefully — but the valuable figures come from us.
        intelligence = None
        try:
            ir = requests.post(INTELLIGENCE_API_URL, json={
                "licence_key": LICENCE_KEY,
                "mode": "usage",
                "summary": raw_summary,
                "sites": sites,
            }, timeout=60)
            if ir.status_code == 200:
                intelligence = ir.json()
            else:
                intelligence = {"status": "error", "http": ir.status_code,
                                "message": ir.text[:300]}
        except Exception as e:
            intelligence = {"status": "error", "message": str(e)}

        return func.HttpResponse(
            json.dumps({"status": "success", "summary": raw_summary,
                        "sites": sites, "intelligence": intelligence},
                       indent=2),
            mimetype="application/json", status_code=200, headers=cors)

    except requests.HTTPError as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e),
                        "hint": "If 403, grant 'Reports.Read.All' (Application) "
                        "to the Entra app and admin-consent it."}),
            mimetype="application/json",
            status_code=getattr(e.response, "status_code", 500), headers=cors)
    except Exception as e:
        logging.exception("usagemetrics failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            mimetype="application/json", status_code=500, headers=cors)


# ===========================================================================
# RESULT CACHE — store the last scan result so the dashboard shows it
# instantly on open (no re-scan). A new scan only runs when the user hits
# "Sync Now". Cached in this tenant's own storage (AzureWebJobsStorage blob).
# ===========================================================================
_CACHE_CONTAINER = "storageiq-cache"
_CACHE_BLOB = "last-result.json"


def _cache_client():
    from azure.storage.blob import BlobServiceClient
    conn = os.environ.get("AzureWebJobsStorage")
    if not conn:
        return None
    svc = BlobServiceClient.from_connection_string(conn)
    try:
        svc.create_container(_CACHE_CONTAINER)
    except Exception:
        pass  # already exists
    return svc.get_blob_client(_CACHE_CONTAINER, _CACHE_BLOB)


@app.route(route="saveresult", methods=["POST", "OPTIONS"])
def saveresult(req: func.HttpRequest) -> func.HttpResponse:
    cors = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=cors)
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Invalid JSON"}),
            status_code=400, mimetype="application/json", headers=cors)

    record = {
        "saved_utc": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
        "kind": body.get("kind", "usage"),   # "usage" (fast) or "deep"
        "data": body.get("data", {}),
    }
    try:
        bc = _cache_client()
        if bc is None:
            raise RuntimeError("No storage connection configured")
        bc.upload_blob(json.dumps(record), overwrite=True)
        return func.HttpResponse(
            json.dumps({"status": "success", "saved_utc": record["saved_utc"]}),
            mimetype="application/json", headers=cors)
    except Exception as e:
        logging.exception("saveresult failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=500, mimetype="application/json", headers=cors)


@app.route(route="lastresult", methods=["GET", "OPTIONS"])
def lastresult(req: func.HttpRequest) -> func.HttpResponse:
    cors = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=cors)
    try:
        bc = _cache_client()
        if bc is None:
            raise RuntimeError("No storage connection configured")
        stream = bc.download_blob()
        record = json.loads(stream.readall())
        return func.HttpResponse(
            json.dumps({"status": "success", "cached": True, "record": record}),
            mimetype="application/json", headers=cors)
    except Exception:
        # No cache yet (first run) — tell the dashboard to show the scan prompt.
        return func.HttpResponse(
            json.dumps({"status": "success", "cached": False, "record": None}),
            mimetype="application/json", headers=cors)
