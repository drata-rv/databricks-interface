"""
Transform merged SCCM device records into the Drata Custom MDM JSON structure.

Fields populated from current data sources:
  alias, externalId, serialNumber, model, platformName, platformVersion,
  appList, antivirusEnabled, antivirusExplanation,
  autoUpdateEnabled, autoUpdateExplanation,
  passwordManagerEnabled, passwordManagerExplanation

Fields populated from the user identity table (joined on Netbios_Name0):
  personnelId          -- User_Princiipal_Name0 (source column has the double-i typo)

Fields set to null -- require additional SCCM tables or data sources:
  firewallEnabled      -- needs gs_firewall or windows services table
  encryptionEnabled    -- needs BitLocker / gs_encryptablevolume table
  screenLockEnabled    -- needs gs_screensaver or policy table
  windowsServices      -- needs gs_services table
  macAddress           -- not present in current tables
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
# Helpers
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
    if 'mac' in s or 'darwin' in s:
        return 'MACOS'
    if 'linux' in s:
        return 'LINUX'
    return 'UNKNOWN'


def _build_app_list(software: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            'name': app.get('product_name_0'),
            'version': app.get('product_version_0'),
            'description': app.get('product_name_0'),
        }
        for app in software
        if app.get('product_name_0')
    ]


def _auto_update(wu: Dict[str, Any]) -> Tuple[bool, str]:
    option = str(wu.get('auoptions0') or '').strip()
    enabled = option == '4'
    explanation = AU_OPTIONS.get(option, 'Unknown')
    return enabled, explanation


# ---------------------------------------------------------------------------
# Main transform
# ---------------------------------------------------------------------------

def _resolve_personnel_id(user: Dict[str, Any]) -> Optional[str]:
    """
    Extract the user's email from the user identity record for use as personnelId.

    Column name note: the source table has a typo -- 'User_Princiipal_Name0' (double-i).
    We try the typo'd spelling first, then the correct spelling as a fallback in case
    Nationwide corrects it in a future schema update.
    """
    return (
        user.get('User_Princiipal_Name0')   # actual column name in source (double-i typo)
        or user.get('User_Principal_Name0') # fallback if typo is corrected
        or user.get('Unique_User_Name0')    # last resort: domain\username
        or None
    )


def to_drata_record(merged: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a merged SCCM record into a Drata Custom MDM payload."""
    device = merged.get('device', {})
    wu = merged.get('windows_update', {})
    software = merged.get('installed_software', [])
    user = merged.get('user', {})

    app_list = _build_app_list(software)
    av_enabled, av_apps = _detect_apps(software, ANTIVIRUS_SIGNATURES)
    pm_enabled, pm_apps = _detect_apps(software, PASSWORD_MANAGER_SIGNATURES)
    auto_update_enabled, auto_update_explanation = _auto_update(wu)

    return {
        # Identity
        'personnelId': _resolve_personnel_id(user),
        'alias': device.get('Name0') or device.get('Netbios_Name0'),
        'externalId': device.get('AADDeviceID') or str(merged.get('resource_id')),
        'serialNumber': device.get('SerialNumber'),
        'model': device.get('CPUType0'),
        'macAddress': None,  # Not present in current SCCM tables

        # Platform
        'platformName': _platform_name(device.get('Operating_System_Name_and0')),
        'platformVersion': device.get('Build01') or device.get('BuildExt'),

        # Antivirus
        'antivirusEnabled': av_enabled,
        'antivirusExplanation': {
            'antivirusApps': av_apps,
        },

        # Applications
        'appList': app_list,
        'browserExtensions': [],  # Not captured by SCCM

        # Auto update
        'autoUpdateEnabled': auto_update_enabled,
        'autoUpdateExplanation': auto_update_explanation,

        # Firewall -- requires gs_firewall or windows services table
        'firewallEnabled': None,
        'firewallExplanation': None,

        # Encryption -- requires BitLocker / gs_encryptablevolume table
        'encryptionEnabled': None,
        'encryptionExplanation': None,

        # Screen lock -- requires gs_screensaver or policy table
        'screenLockEnabled': None,
        'screenLockExplanation': None,
        'screenLockTime': None,

        # Password manager
        'passwordManagerEnabled': pm_enabled,
        'passwordManagerExplanation': {
            'passwordManagerApps': pm_apps,
        },

        # Windows services -- requires gs_services table
        'windowsServices': [],
    }


def transform_all(merged_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform a list of merged records into Drata MDM payloads."""
    return [to_drata_record(r) for r in merged_records]
