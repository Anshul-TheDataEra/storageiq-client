# StorageIQ CLIENT scanner agent — container image for customer deployment.
#
# Ships to the customer so they can run the scanner inside THEIR OWN Azure.
# The savings/ranking IP is NOT in here — it lives behind our Intelligence API.
#
# Build:  docker build -t storageiq-client:latest .
# Run:
#   docker run -p 8080:80 \
#     -e AzureWebJobsStorage="<customer-storage-connection-string>" \
#     -e TENANT_ID=... -e CLIENT_ID=... -e CLIENT_SECRET=... \
#     -e INTELLIGENCE_API_URL="https://storageiq-intelligence.azurewebsites.net/api/intelligence" \
#     -e LICENCE_KEY="<customer-licence-key>" \
#     storageiq-client:latest
#
# AzureWebJobsStorage is REQUIRED (Durable checkpoint/queue state).
# Never bake secrets into the image — pass them at run time / via Key Vault.

FROM mcr.microsoft.com/azure-functions/python:4-python3.11

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /home/site/wwwroot
