#!/bin/bash
# infra/deploy.sh

# Carica le variabili dal file .env (senza esportarle globalmente per tutto il sistema)
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "⚠️  File .env non trovato! Assicurati di avere le variabili d'ambiente impostate."
fi

# Recupero dinamico ID Ambiente (così non lo hardcodiamo)
echo "🔍 Recupero configurazione Azure..."
export ENV_ID=$(az containerapp env show --name cae-preventivatore --resource-group rg-preventivatore-prod --query id --output tsv)

# Se la password non è nel .env, proviamo a recuperarla da Azure
if [ -z "$ACR_PASSWORD" ]; then
    export ACR_PASSWORD=$(az acr credential show --name acrprev14340 --resource-group rg-preventivatore-prod --query "passwords[0].value" --output tsv)
fi

echo "🚀 Generazione configurazione finale..."

# Usa envsubst per sostituire le variabili nel template
# Nota: Creiamo un file temporaneo che viene subito cancellato dopo l'uso
envsubst < infra/preventivatore.yaml > infra/preventivatore.generated.yaml

echo "☁️  Deploy su Azure..."
az containerapp job update \
  --name job-preventivatore \
  --resource-group rg-preventivatore-prod \
  --yaml infra/preventivatore.generated.yaml

# Pulizia: Rimuovi il file con i segreti
rm infra/preventivatore.generated.yaml

echo "✅ Deploy Completato e file temporanei rimossi."
