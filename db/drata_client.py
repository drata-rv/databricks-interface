"""
Drata Custom Device Connection API client.

Pushes device records to Drata using the Custom Device Connection endpoint.
Batches records to stay within API limits, retries transient errors, and
collects per-batch failures without aborting the entire push.

Required env vars:
  DRATA_API_KEY       -- Bearer token for the Drata public API
  DRATA_CONNECTION_ID -- UUID of the Custom Device Connection in Drata

Endpoint reference:
  developers.drata.com/openapi/reference/v2/tag/Devices
  Operation: DevicesPublicV2Controller_createDeviceForCustomConnection
  Verify the exact path below before first use.
"""

import time
from typing import Any, Dict, List

# Batch size per API call. Adjust if Drata imposes a lower limit.
_BATCH_SIZE = 100
_MAX_RETRIES = 3
_RETRY_DELAYS = (5, 15)  # seconds before attempt 2 and attempt 3

# Base URL for the Drata public API.
_BASE_URL = "https://public-api.drata.com"

# Endpoint for the Custom Device Connection batch push.
# Verify this path from the Drata OpenAPI spec before first use.
_PUSH_PATH = "/v2/devices/custom-connection"


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
        Push all records to Drata in batches of _BATCH_SIZE.

        Returns a summary dict: {'total': int, 'batches': int, 'errors': list}.
        errors is a list of {'batch': int, 'error': str} for failed batches.
        """
        url = f"{_BASE_URL}{_PUSH_PATH}/{self._connection_id}"
        errors: List[Dict[str, Any]] = []
        batches = 0
        for i in range(0, len(records), _BATCH_SIZE):
            chunk = records[i: i + _BATCH_SIZE]
            batches += 1
            self._push_one_batch(url, chunk, batches, errors)
        return {'total': len(records), 'batches': batches, 'errors': errors}

    def _push_one_batch(
        self,
        url: str,
        chunk: List[Dict[str, Any]],
        batch_num: int,
        errors: List[Dict[str, Any]],
    ) -> None:
        import requests

        last_err = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._session.post(url, json=chunk, timeout=self._timeout)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get('Retry-After', _RETRY_DELAYS[min(attempt - 1, 1)]))
                    print(f"  [RATE LIMIT] batch {batch_num}, retrying in {retry_after}s ...")
                    time.sleep(retry_after)
                    continue

                if resp.status_code >= 400 and resp.status_code < 500:
                    # 4xx errors (other than 429) are not retried -- bad data or auth
                    msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    print(f"  [FAIL] batch {batch_num}: {msg}")
                    errors.append({'batch': batch_num, 'error': msg})
                    return

                resp.raise_for_status()
                return

            except Exception as e:
                last_err = e
                if attempt < _MAX_RETRIES:
                    wait = _RETRY_DELAYS[attempt - 1]
                    print(f"  [RETRY {attempt}/{_MAX_RETRIES}] batch {batch_num} failed, retrying in {wait}s ...")
                    time.sleep(wait)

        msg = str(last_err)
        print(f"  [FAIL] batch {batch_num} failed after {_MAX_RETRIES} attempts: {msg}")
        errors.append({'batch': batch_num, 'error': msg})
