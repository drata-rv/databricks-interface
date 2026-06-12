"""
Drata Custom Device Connection API client.

Pushes device records to Drata one at a time via the Custom Device Connection endpoint.
Retries transient errors and collects per-record failures without aborting the entire push.

Required env vars:
  DRATA_API_KEY       -- Bearer token for the Drata public API
  DRATA_CONNECTION_ID -- ID of the Custom Device Connection in Drata

Endpoint:
  POST https://public-api.drata.com/public/v2/custom-connections/{connectionId}/devices
  One device object per request.
"""

import time
from typing import Any, Dict, List

_MAX_RETRIES = 3
_RETRY_DELAYS = (5, 15)  # seconds before attempt 2 and attempt 3

_BASE_URL = "https://public-api.drata.com"
_PUSH_PATH = "/public/v2/custom-connections/{connection_id}/devices"


class DrataClient:
    """Push Drata Custom Device Connection records via the Drata public API."""

    def __init__(self, api_key: str, connection_id: str, timeout: int = 30) -> None:
        self._api_key = api_key
        self._connection_id = connection_id
        self._timeout = timeout
        self._session = self._build_session()

    def _build_session(self) -> Any:
        import requests
        s = requests.Session()
        s.headers.update({
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        })
        return s

    def push_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Push all records to Drata one at a time (API accepts one device per POST).

        Returns a summary dict: {'total': int, 'pushed': int, 'errors': list}.
        errors is a list of {'index': int, 'error': str} for failed records.
        """
        url = f"{_BASE_URL}{_PUSH_PATH.format(connection_id=self._connection_id)}"
        errors: List[Dict[str, Any]] = []
        pushed = 0
        for i, record in enumerate(records):
            success = self._push_one_record(url, record, i + 1, len(records), errors)
            if success:
                pushed += 1
        return {'total': len(records), 'pushed': pushed, 'errors': errors}

    def _push_one_record(
        self,
        url: str,
        record: Dict[str, Any],
        index: int,
        total: int,
        errors: List[Dict[str, Any]],
    ) -> bool:
        import requests

        pid    = record.get('personnelId', '(none)')
        alias  = record.get('alias', '(none)')
        ext_id = record.get('externalId', '(none)')
        print(f"  [{index}/{total}] personnelId={pid}  alias={alias}  externalId={ext_id}")

        last_err = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._session.post(url, json=record, timeout=self._timeout)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get('Retry-After', _RETRY_DELAYS[min(attempt - 1, 1)]))
                    print(f"    [RATE LIMIT] retrying in {retry_after}s ...")
                    time.sleep(retry_after)
                    continue

                if 400 <= resp.status_code < 500:
                    msg = f"HTTP {resp.status_code}: {resp.text}"
                    print(f"    [FAIL] {msg}")
                    errors.append({'index': index, 'personnelId': pid, 'alias': alias, 'error': msg})
                    return False

                if resp.status_code >= 500:
                    body = resp.text
                    print(f"    [5XX attempt {attempt}/{_MAX_RETRIES}] HTTP {resp.status_code}: {body}")
                    last_err = f"HTTP {resp.status_code}: {body}"
                    if attempt < _MAX_RETRIES:
                        wait = _RETRY_DELAYS[attempt - 1]
                        print(f"    retrying in {wait}s ...")
                        time.sleep(wait)
                    continue

                print(f"    [OK] HTTP {resp.status_code}")
                return True

            except Exception as e:
                last_err = str(e)
                if attempt < _MAX_RETRIES:
                    wait = _RETRY_DELAYS[attempt - 1]
                    print(f"    [RETRY {attempt}/{_MAX_RETRIES}] {e} -- retrying in {wait}s ...")
                    time.sleep(wait)

        print(f"    [FAIL] all {_MAX_RETRIES} attempts exhausted: {last_err}")
        errors.append({'index': index, 'personnelId': pid, 'alias': alias, 'error': last_err})
        return False

    def fetch_current_personnel_emails(self, statuses=None, max_records=None):
        """
        Fetch email addresses for all active Drata personnel.
        Returns a frozenset of lowercase email strings.

        max_records caps how many personnel records are fetched. When set, the
        filter is partial -- it only covers the first max_records entries returned
        by the API. For a complete filter use the personnel cache (--refresh-personnel).
        """
        if statuses is None:
            statuses = ['CURRENT_EMPLOYEE', 'CURRENT_CONTRACTOR']
        url = f"{_BASE_URL}/public/v2/personnel"
        emails = set()
        cursor = None
        page = 0
        while True:
            page += 1
            params = {'size': 500, 'expand[]': 'user'}
            for s in statuses:
                params.setdefault('employmentStatus[]', []).append(s)
            if cursor:
                params['cursor'] = cursor
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            body = resp.json()
            for p in body.get('data', []):
                email = (p.get('user') or {}).get('email')
                if email:
                    emails.add(email.lower())
            cursor = body.get('pagination', {}).get('cursor')
            print(f"  Page {page}: {len(body.get('data', []))} personnel, {len(emails)} emails so far ...")
            if max_records and len(emails) >= max_records:
                print(f"  [CAP] Personnel fetch stopped at {len(emails)} (max_records={max_records}).")
                print(f"        Filter is partial -- run with --refresh-personnel for a complete roster.")
                break
            if not cursor:
                break
        return frozenset(emails)
