import json
import os
from datetime import date

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings


DEFAULT_EXCHANGE_RATE_URL = "https://open.er-api.com/v6/latest/{base_currency}"


def fetch_exchange_rates(
	base_currency: str = "EUR",
	url: str | None = None,
) -> dict[str, float]:
	"""Récupère les taux du jour, exprimés en devise cible par devise de base."""

	base_currency = base_currency.upper()
	endpoint = (
		url or os.getenv("EXCHANGE_RATE_URL", DEFAULT_EXCHANGE_RATE_URL)
	).format(base_currency=base_currency)
	response = requests.get(endpoint, timeout=15)
	response.raise_for_status()
	payload = response.json()

	if payload.get("result") != "success" or not isinstance(payload.get("rates"), dict):
		raise ValueError("La réponse du service de taux de change est invalide")

	return {
		currency.upper(): float(rate)
		for currency, rate in payload["rates"].items()
	}


def create_blob_service_client() -> BlobServiceClient:
	"""Construit le client Azure Blob depuis les variables d'environnement."""

	account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
	account_url = (
		account_name
		if account_name.startswith("http")
		else f"https://{account_name}.blob.core.windows.net"
	)
	return BlobServiceClient(
		account_url=account_url,
		credential=os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
	)


def upload_exchange_rates(
	rates: dict[str, float],
	base_currency: str = "EUR",
	container_name: str | None = None,
	blob_name: str | None = None,
	client: BlobServiceClient | None = None,
) -> str:
	"""Dépose les taux dans la zone raw/reference d'ADLS Gen2 au format JSON."""

	base_currency = base_currency.upper()
	container_name = container_name or os.getenv("AZURE_RAW_CONTAINER", "raw")
	blob_name = blob_name or (
		f"{os.getenv('AZURE_RAW_REFERENCE_PATH', 'reference').strip('/')}/"
		f"exchange_rates_{date.today().isoformat()}.json"
	)
	payload = {
		"date": date.today().isoformat(),
		"base_currency": base_currency,
		"rates": rates,
	}
	data = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")

	blob_client = (client or create_blob_service_client()).get_blob_client(
		container=container_name,
		blob=blob_name,
	)
	blob_client.upload_blob(
		data,
		overwrite=True,
		content_settings=ContentSettings(content_type="application/json"),
	)
	return f"{container_name}/{blob_name}"


def main() -> None:
	"""Récupère les taux du jour et les charge dans ADLS Gen2."""

	base_currency = os.getenv("EXCHANGE_RATE_BASE", "EUR").upper()
	rates = fetch_exchange_rates(base_currency)
	location = upload_exchange_rates(rates, base_currency)
	print(f"Taux déposés dans ADLS Gen2 : {location}")


if __name__ == "__main__":
	main()
