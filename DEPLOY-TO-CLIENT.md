# Deploy the StorageIQ Scanner Agent into a Customer's Azure

This guide deploys **Part B — the scanner agent** into the **customer's own
Azure tenant**. The scanner reads their SharePoint and sends only anonymised
aggregate numbers to **our Intelligence API** (Part A), which returns the
savings / ranking. The customer never receives the savings logic.

> Do this **once per customer**. It takes ~20–30 minutes.

---

## What you need before you start

| Item | From whom | Notes |
|------|-----------|-------|
| Customer Azure sign-in (Contributor on a subscription) | Customer | You deploy into *their* subscription |
| Customer Entra app: **Tenant ID / Client ID / Client Secret** | Customer admin | Read-only Graph app (see permissions below) |
| A **licence key** for this customer | You | Add it to our Intelligence API's `LICENCE_KEYS` first |
| Our Intelligence API URL | You | `https://storageiq-intelligence.azurewebsites.net/api/intelligence` |

### Graph permissions on the customer's Entra app (application, admin-consented)
`Sites.Read.All`, `Files.Read.All`, `Reports.Read.All`, `Organization.Read.All`.
All read-only. Nothing is written during a scan.

---

## Step 0 — Issue a licence key for this customer (on OUR side)

The scanner is useless without a valid key. On **our** Intelligence API:

```powershell
# Add this customer's key to the allow-list (comma-separated).
az functionapp config appsettings set `
  --name storageiq-intelligence `
  --resource-group rg-dataera-grc-dev `
  --settings LICENCE_KEYS="DEMO-LICENCE,CUST-ACME-2026"
```

Give the customer their key value (e.g. `CUST-ACME-2026`) — it goes into the
agent's settings in Step 3.

---

## Step 1 — Sign in to the CUSTOMER's Azure

```powershell
az login                       # sign in as / with the customer
az account set --subscription "<customer-subscription-id-or-name>"
az account show                # confirm you're in the customer's subscription
```

---

## Step 2 — Create the resources in the customer tenant

Pick names + a region (use the customer's data region).

```powershell
$RG   = "storageiq-rg"
$LOC  = "canadacentral"
$SA   = "storageiqcust$((Get-Random -Maximum 9999))"   # 3-24 lowercase, globally unique
$APP  = "storageiq-scan-<customer>"                    # globally unique

az group create --name $RG --location $LOC

az storage account create --name $SA --resource-group $RG `
  --location $LOC --sku Standard_LRS

az functionapp create --name $APP --resource-group $RG `
  --storage-account $SA --consumption-plan-location $LOC `
  --runtime python --runtime-version 3.11 --functions-version 4 --os-type Linux
```

> Durable Functions uses the storage account for checkpoint/queue state — this
> is what makes the long scan resumable and survive cold starts. No VM needed.

---

## Step 3 — Configure the agent (credentials + our API + licence)

```powershell
az functionapp config appsettings set --name $APP --resource-group $RG --settings `
  TENANT_ID="<customer-tenant-id>" `
  CLIENT_ID="<customer-client-id>" `
  CLIENT_SECRET="<customer-client-secret>" `
  INTELLIGENCE_API_URL="https://storageiq-intelligence.azurewebsites.net/api/intelligence" `
  LICENCE_KEY="CUST-ACME-2026"
```

> Best practice: store `CLIENT_SECRET` in the customer's **Key Vault** and use a
> Key Vault reference instead of the raw value.

---

## Step 4 — Deploy the code

From this folder (`storageiq-durable-client/`):

```powershell
func azure functionapp publish $APP --python
```

**Option B — Docker** (if the customer prefers containers):

```powershell
az acr build -r <customerRegistry> -t storageiq-client:latest .
# then point a Function App (container) / Container App at that image and set
# the same app settings as Step 3.
```

---

## Step 4b — Allow the dashboard to call the agent (CORS)

The StorageIQ dashboard runs in the browser and calls the agent's HTTP
endpoints. Allow that cross-origin call on the agent:

```powershell
# Simplest: allow the dashboard origin. Use "*" only if the dashboard is
# rendered from a blob/opaque origin (then no specific origin can be listed).
az functionapp cors add --name $APP --resource-group $RG `
  --allowed-origins "https://<customer>.sharepoint.com"
```

> If the dashboard is embedded as an inline blob (opaque origin), a specific
> origin cannot be matched and you may need `"*"`. Endpoints are read-only.

---

## Step 5 — Run a scan and watch progress

```powershell
# Start (credentials fall back to the app settings from Step 3).
curl -X POST "https://$APP.azurewebsites.net/api/startscan"

# The response includes an instance id + status URLs. Poll progress:
curl "https://$APP.azurewebsites.net/api/scanstatus?id=<instanceId>"
```

`customStatus` shows `{ sites_done, sites_total, phase }` so a dashboard can
render a live progress bar. When `phase` is `done`, the `output.intelligence`
field holds the savings + ranking returned by our API.

---

## Step 6 — Point the dashboard at this customer's agent

The **same** StorageIQ dashboard build works for every customer — you just tell
it which agent to use. No rebuild needed. Resolution order in the dashboard:

1. **`?scan=` query param** (easiest, per-customer) — append the agent base URL:
   ```
   https://<dashboard-url>/?scan=https://<APP>.azurewebsites.net/api
   ```
   Give the customer this URL (or set it as the SPFx `costIntelligenceUrl`
   property so the button opens it). The "Connect & Scan Tenant" button then
   drives THIS customer's agent and shows the live progress bar.
2. `window.__SCAN_BASE__` injected by the host page.
3. Falls back to our internal scanner (demo/default).

### Deploying the dashboard UI into the customer's SharePoint
Upload the StorageIQ SPFx package (`sp-version-archival.sppkg`) to the
customer's **App Catalog** → Deploy → add to the target site. Set the
extension's `costIntelligenceUrl` property to the dashboard URL **with the
`?scan=` param above**, so the in-SharePoint button opens the dashboard already
wired to this customer's agent.

---

## What crosses the boundary (for the customer's security review)

- **Leaves the customer tenant:** anonymised aggregate numbers per site
  (storage GB, version GB, cold GB, file/version counts) + site display names
  + the licence key. **No file contents, no file paths, no user identities.**
- **Comes back:** savings figures, ranked sites, recommendations.
- **Stays in the customer tenant:** all raw SharePoint data, the scan itself,
  and (if enabled later) the archival execution.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `403` from Intelligence API | Licence key missing/typo | Re-check `LICENCE_KEY` vs. `LICENCE_KEYS` (Step 0/3) |
| `Auth failed` on scan | Entra app creds wrong / no consent | Re-check TENANT/CLIENT/SECRET + admin consent |
| Sites missing from report | Permissions too narrow | Ensure `Sites.Read.All` + `Files.Read.All` granted |
| Scan seems stuck | Large tenant + throttling | Normal — it backs off on 429 and resumes; watch `sites_done` |
