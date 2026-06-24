"""
Transform merged SCCM device records into the Drata Custom MDM JSON structure.

Pipeline stages:
  extract_features(merged) -- triangulate all boolean signals from raw SCCM data
  format_for_drata(features) -- map extracted features to the Drata JSON shape

Fields populated from current data sources:
  alias, externalId, serialNumber, model, platformName, platformVersion,
  appList, antivirusEnabled, antivirusExplanation,
  autoUpdateEnabled, autoUpdateExplanation,
  passwordManagerEnabled, passwordManagerExplanation

Fields populated from the user identity table (joined on Netbios_Name0):
  personnelId          -- User_Princiipal_Name0 (source column has the double-i typo)

Fields set to null -- require additional SCCM tables (uncomment in TABLE_REGISTRY to enable):
  firewallEnabled      -- needs t_sccm_gs_services (mpssvc / Windows Firewall service)
  encryptionEnabled    -- needs t_sccm_gs_bitlockerdetails (ProtectionStatus, EncryptionPercentage)
  screenLockEnabled    -- needs t_sccm_gs_screensaversettings (IsEnabled, IsSecure, WaitInterval)
  windowsServices      -- needs t_sccm_gs_services
  macAddress           -- needs t_sccm_gs_networkadapterconfiguration
  browserExtensions    -- not captured by SCCM
"""

from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Known application signatures
# ---------------------------------------------------------------------------

ANTIVIRUS_SIGNATURES = [
    'crowdstrike', 'defender', 'norton', 'mcafee', 'symantec',
    'bitdefender', 'kaspersky', 'malwarebytes', 'eset', 'avast',
    'avg', 'sophos', 'trend micro', 'cylance', 'sentinel one',
    'sentinelone', 'carbon black', 'webroot',
]

PASSWORD_MANAGER_SIGNATURES = [
    '1password', 'lastpass', 'bitwarden', 'dashlane', 'keepass',
    'roboform', 'keeper', 'nordpass', 'enpass',
]

# auoptions0 values from Windows Update registry
AU_OPTIONS: Dict[str, str] = {
    '1': 'Disabled',
    '2': 'Notify before download',
    '3': 'Auto download, notify before install',
    '4': 'Auto download and install',
}


# ---------------------------------------------------------------------------
# Existing helpers (unchanged)
# ---------------------------------------------------------------------------

def _match_signatures(app_name: str, signatures: List[str]) -> bool:
    name_lower = (app_name or '').lower()
    return any(sig in name_lower for sig in signatures)


def _detect_apps(
    software: List[Dict[str, Any]],
    signatures: List[str],
) -> Tuple[bool, List[str]]:
    """Return (found: bool, matched_app_names: list)."""
    matched = []
    seen = set()
    for app in software:
        name = app.get('product_name_0') or ''
        if name and name not in seen and _match_signatures(name, signatures):
            matched.append(name)
            seen.add(name)
    return len(matched) > 0, matched


def _platform_name(os_string: Optional[str]) -> str:
    s = (os_string or '').lower()
    if 'windows' in s:
        return 'WINDOWS'
    if 'mac' in s or 'darwin' in s or 'macos' in s:
        return 'MACOS'
    if 'linux' in s:
        return 'LINUX'
    if 'android' in s:
        return 'ANDROID'
    # iOS/unknown: SCCM is Windows-only so these should not appear in practice;
    # fall back to WINDOWS as the closest valid enum value for unrecognized SCCM OS strings.
    return 'WINDOWS'


def _build_app_list(software: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set = set()
    result = []
    for app in software:
        name = app.get('product_name_0')
        if not name:
            continue
        version = app.get('product_version_0') or 'Unknown'
        key = (name, version)
        if key not in seen:
            seen.add(key)
            result.append({
                'name': name,
                'version': version,
                'description': name,
            })
    return result


def _auto_update(wu: Dict[str, Any], device: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    if device:
        if device.get('disable_windows_update_access') or device.get('do_not_connect_to_wu_locations'):
            return False, 'Windows Update disabled by policy'
    option = str(wu.get('auoptions0') or '').strip()
    enabled = option == '4'
    explanation = AU_OPTIONS.get(option, 'Unknown')
    return enabled, explanation


def _resolve_personnel_id(user: Dict[str, Any]) -> Optional[str]:
    """
    Extract the user's email from the user identity record for use as personnelId.

    Drata requires the format 'email:<emailAddress>' for email-based personnelId values.

    Column name note: xlsx source has a typo ('User_Princiipal_Name0', double-i).
    Databricks t_sccm_r_user uses snake_case ('user_principal_name0').
    All three variants are tried in order.
    """
    value = (
        user.get('User_Princiipal_Name0')   # xlsx: double-i typo in source column
        or user.get('User_Principal_Name0') # xlsx: correct spelling fallback
        or user.get('user_principal_name0') # Databricks t_sccm_r_user (snake_case)
        or None
        # Unique_User_Name0 is domain\username (e.g. NATIONWIDE\BONKB6) -- not a valid
        # Drata personnelId format, so we don't use it here.
    )
    if value and '@' in value:
        return f"email:{value}"
    return None


# ---------------------------------------------------------------------------
# Blocked-field helpers -- each returns None when its source table is absent
# ---------------------------------------------------------------------------

def _extract_encryption(
    bitlocker: Optional[Dict[str, Any]],
) -> Tuple[Optional[bool], Optional[Dict[str, Any]]]:
    """Derive encryptionEnabled from a BitLocker details row. Returns (None, None) if table absent."""
    if not bitlocker:
        return None, None
    protected = str(bitlocker.get('ProtectionStatus') or '').strip()
    pct_raw = bitlocker.get('EncryptionPercentage')
    try:
        pct = int(pct_raw) if pct_raw is not None else None
    except (ValueError, TypeError):
        pct = None
    enabled = protected == '1' and pct == 100
    explanation = {
        'bootPartitionEncryptionDetails': {
            'partitionFileVault2Percent': pct,
            'partitionFileVault2State': 'ENCRYPTED' if enabled else 'DECRYPTED',
            'partitionName': bitlocker.get('DriveLetter') or 'C:',
        }
    }
    return enabled, explanation


def _extract_screen_lock(
    screensaver: Optional[Dict[str, Any]],
) -> Tuple[Optional[bool], Optional[str], Optional[int]]:
    """Derive screenLockEnabled from a screensaver settings row. Returns (None, None, None) if absent."""
    if not screensaver:
        return None, None, None
    is_enabled = screensaver.get('IsEnabled')
    is_secure = screensaver.get('IsSecure')  # requires password on screensaver dismiss
    wait_raw = screensaver.get('WaitInterval')
    try:
        wait = int(wait_raw) if wait_raw is not None else None
    except (ValueError, TypeError):
        wait = None
    enabled = bool(is_enabled) and bool(is_secure)
    if wait is not None:
        explanation = f"ScreenLock delay is {wait} minutes"
    elif enabled:
        explanation = 'Enabled'
    else:
        explanation = 'Disabled'
    return enabled, explanation, wait


def _extract_firewall(
    services: Optional[List[Dict[str, Any]]],
) -> Tuple[Optional[bool], Optional[str]]:
    """Derive firewallEnabled from Windows services rows. Returns (None, None) if table absent."""
    if services is None:
        return None, None
    for svc in services:
        name = (svc.get('Name') or svc.get('name') or '').lower()
        if 'mpssvc' in name or 'windows firewall' in name:
            status = svc.get('Status') or svc.get('status') or ''
            enabled = str(status).lower() == 'running'
            return enabled, str(status)
    return False, 'Service not found'


def _build_windows_services(
    services: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Map raw service rows to the Drata windowsServices shape. Returns [] if table absent."""
    if not services:
        return []
    return [
        {
            'description': svc.get('Description') or svc.get('description') or '',
            'name': svc.get('Name') or svc.get('name') or '',
            'startType': svc.get('StartType') or svc.get('startType') or '',
            'status': svc.get('Status') or svc.get('status') or '',
        }
        for svc in services
    ]


def _extract_mac_address(
    network_adapter: Optional[Dict[str, Any]],
) -> Optional[str]:
    """Return the MAC address from a network adapter config row. Returns None if table absent."""
    if not network_adapter:
        return None
    return (
        network_adapter.get('MACAddress0')
        or network_adapter.get('MACAddress')
        or network_adapter.get('macaddress')
        or None
    )


# ---------------------------------------------------------------------------
# Stage 3: Feature extraction -- all triangulation logic lives here
# ---------------------------------------------------------------------------

def extract_features(merged: Dict[str, Any]) -> Dict[str, Any]:
    """
    Derive all boolean signals and explanation data from a merged SCCM record.

    Optional table keys (bitlocker, screensaver, services, network_adapter) are
    present in merged only when the corresponding table was pulled. Each helper
    returns None/empty when its input is None, preserving null output until the
    real table is wired in via TABLE_REGISTRY.
    """
    device = merged.get('device', {})
    wu = merged.get('windows_update', {})
    software = merged.get('installed_software', [])
    user = merged.get('user', {})

    av_enabled, av_apps = _detect_apps(software, ANTIVIRUS_SIGNATURES)
    sense_id = (device.get('sense_id') or '').strip()
    if sense_id:
        av_enabled = True
        if 'Microsoft Defender for Endpoint' not in av_apps:
            av_apps = [*av_apps, 'Microsoft Defender for Endpoint']
    pm_enabled, pm_apps = _detect_apps(software, PASSWORD_MANAGER_SIGNATURES)
    au_enabled, au_explanation = _auto_update(wu, device)
    fw_enabled, fw_explanation = _extract_firewall(merged.get('services'))
    enc_enabled, enc_explanation = _extract_encryption(merged.get('bitlocker'))
    sl_enabled, sl_explanation, sl_time = _extract_screen_lock(merged.get('screensaver'))

    return {
        'resource_id': merged.get('resource_id'),
        'device': device,
        'user': user,
        'av_enabled': av_enabled,
        'av_apps': av_apps,
        'pm_enabled': pm_enabled,
        'pm_apps': pm_apps,
        'au_enabled': au_enabled,
        'au_explanation': au_explanation,
        'app_list': _build_app_list(software),
        'fw_enabled': fw_enabled,
        'fw_explanation': fw_explanation,
        'enc_enabled': enc_enabled,
        'enc_explanation': enc_explanation,
        'sl_enabled': sl_enabled,
        'sl_explanation': sl_explanation,
        'sl_time': sl_time,
        'windows_services': _build_windows_services(merged.get('services')),
        'mac_address': _extract_mac_address(merged.get('network_adapter')),
    }


# ---------------------------------------------------------------------------
# Stage 4: Drata format assembly -- mapping only, no logic
# ---------------------------------------------------------------------------

def format_for_drata(features: Dict[str, Any]) -> Dict[str, Any]:
    """Map an extracted-features dict to the Drata Custom Device Connection JSON shape."""
    device = features['device']
    user = features['user']
    return {
        'personnelId': _resolve_personnel_id(user),
        'serialNumber': device.get('SerialNumber') or device.get('serial_number'),
        'alias': (
            device.get('Netbios_Name0') or device.get('Name0')
            or device.get('netbios_name0') or device.get('name0')
        ),
        'externalId': (
            device.get('SerialNumber') or device.get('serial_number')
            or device.get('AADDeviceID') or device.get('aad_device_id')
            or (str(features['resource_id']) if features.get('resource_id') is not None else None)
        ),
        'model': device.get('CPUType0'),
        'macAddress': features['mac_address'],
        'platformName': _platform_name(device.get('Operating_System_Name_and0')),
        'platformVersion': device.get('Build01') or device.get('BuildExt') or 'Unknown',
        'antivirusEnabled': features['av_enabled'],
        'antivirusExplanation': {'antivirusApps': features['av_apps']},
        'appList': features['app_list'],
        'browserExtensions': [],
        'autoUpdateEnabled': features['au_enabled'],
        'autoUpdateExplanation': features['au_explanation'],
        'firewallEnabled': features['fw_enabled'],
        'firewallExplanation': features['fw_explanation'],
        'encryptionEnabled': features['enc_enabled'],
        'encryptionExplanation': features['enc_explanation'],
        'screenLockEnabled': features['sl_enabled'],
        'screenLockExplanation': features['sl_explanation'],
        'screenLockTime': features['sl_time'],
        'passwordManagerEnabled': features['pm_enabled'],
        'passwordManagerExplanation': {'passwordManagerApps': features['pm_apps']},
        'windowsServices': features['windows_services'],
    }


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------

_TEST_PASSING_FIELDS: Dict[str, Any] = {
    'antivirusEnabled': True,
    'antivirusExplanation': {'antivirusApps': ['Test Mode']},
    'autoUpdateEnabled': True,
    'autoUpdateExplanation': 'Auto download and install',
    'passwordManagerEnabled': True,
    'passwordManagerExplanation': {'passwordManagerApps': ['Test Mode']},
    'encryptionEnabled': True,
    'encryptionExplanation': {
        'bootPartitionEncryptionDetails': {
            'partitionFileVault2Percent': 100,
            'partitionFileVault2State': 'ENCRYPTED',
            'partitionName': 'C:',
        }
    },
    'screenLockEnabled': True,
    'screenLockExplanation': 'ScreenLock delay is 15 minutes',
    'screenLockTime': 15,
}


def apply_sandbox_overrides(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Replace @nationwide.com with @sandbox.nationwide.com in personnelId for sandbox testing."""
    result = []
    for r in records:
        pid = r.get('personnelId')
        if isinstance(pid, str) and '@nationwide.com' in pid:
            r = {**r, 'personnelId': pid.replace('@nationwide.com', '@sandbox.nationwide.com')}
        result.append(r)
    return result


def apply_test_overrides(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Force all 5 Drata monitoring fields to a passing state.

    Identity fields (personnelId, alias, externalId, serialNumber, etc.) are
    preserved from the real records so the push targets actual users/devices.
    Only the 5 monitored boolean fields and their explanations are overridden.
    """
    return [{**r, **_TEST_PASSING_FIELDS} for r in records]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def to_drata_record(merged: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a merged SCCM record into a Drata Custom MDM payload."""
    return format_for_drata(extract_features(merged))


def transform_all(merged_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform a list of merged records into Drata MDM payloads."""
    return [format_for_drata(extract_features(r)) for r in merged_records]
