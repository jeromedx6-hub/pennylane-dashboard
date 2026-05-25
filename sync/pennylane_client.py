import os
import requests
from datetime import date, timedelta
from typing import Optional

BASE_URL = "https://app.pennylane.com/api/external/v1"


class PennylaneClient:
    def __init__(self, api_token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{BASE_URL}/{endpoint}"
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, endpoint: str, params: dict = None) -> list:
        params = params or {}
        params.setdefault("per_page", 100)
        results = []
        page = 1
        while True:
            params["page"] = page
            data = self._get(endpoint, params)
            items = data.get("transactions") or data.get("items") or data.get("invoices") or []
            results.extend(items)
            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1
        return results

    def get_customer_invoices(self, date_from: date, date_to: date) -> list:
        """Factures clients (CA encaissé)."""
        return self._paginate("customer_invoices", {
            "filter[date][gte]": date_from.isoformat(),
            "filter[date][lte]": date_to.isoformat(),
            "filter[status]": "paid",
        })

    def get_supplier_invoices(self, date_from: date, date_to: date) -> list:
        """Factures fournisseurs (charges)."""
        return self._paginate("supplier_invoices", {
            "filter[date][gte]": date_from.isoformat(),
            "filter[date][lte]": date_to.isoformat(),
        })

    def get_transactions(self, date_from: date, date_to: date) -> list:
        """Transactions bancaires brutes."""
        return self._paginate("transactions", {
            "filter[date][gte]": date_from.isoformat(),
            "filter[date][lte]": date_to.isoformat(),
        })

    def get_categories(self) -> list:
        """Toutes les catégories analytiques configurées."""
        data = self._get("plan_items")
        return data.get("plan_items", [])
