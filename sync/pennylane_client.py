import os
import requests
from datetime import date
import time

BASE_URL = "https://app.pennylane.com/api/external/v2"
RATE_LIMIT_DELAY = 0.26  # 4 req/s max


class PennylaneClient:
    def __init__(self, api_token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
        })

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{BASE_URL}/{endpoint}"
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def _paginate_cursor(self, endpoint: str, params: dict = None) -> list:
        """Pagination cursor (API v2 : has_more / next_cursor)."""
        params = params or {}
        params.setdefault("per_page", 100)
        results = []
        while True:
            data = self._get(endpoint, params)
            items = data.get("items", [])
            results.extend(items)
            time.sleep(RATE_LIMIT_DELAY)
            if not data.get("has_more"):
                break
            params["cursor"] = data["next_cursor"]
        return results

    def get_customer_invoices(self, date_from: date, date_to: date) -> list:
        return self._paginate_cursor("customer_invoices", {
            "date_gte": date_from.isoformat(),
            "date_lte": date_to.isoformat(),
            "status":   "paid",
        })

    def get_supplier_invoices(self, date_from: date, date_to: date) -> list:
        return self._paginate_cursor("supplier_invoices", {
            "date_gte": date_from.isoformat(),
            "date_lte": date_to.isoformat(),
        })

    def get_invoice_categories(self, invoice_type: str, invoice_id: int) -> list:
        """Retourne les catégories d'une facture (appel séparé requis en v2)."""
        try:
            data = self._get(f"{invoice_type}/{invoice_id}/categories")
            time.sleep(RATE_LIMIT_DELAY)
            return data.get("items", [])
        except Exception:
            return []

    def get_categories(self) -> list:
        data = self._get("categories")
        return data.get("items", [])
