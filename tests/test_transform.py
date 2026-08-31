"""
db/transform.py tests for the newly-wired Encrypted Volume (encryptionEnabled) and
Computer System (model) fields. Both are pure functions -- no mocking needed.
"""

from db import transform


def test_extract_encryption_none_when_table_absent():
    assert transform._extract_encryption(None) == (None, None)


def test_extract_encryption_protected():
    enabled, explanation = transform._extract_encryption({
        'protection_status0': '1', 'drive_letter0': 'C:',
    })
    assert enabled is True
    assert explanation['bootPartitionEncryptionDetails']['partitionFileVault2State'] == 'ENCRYPTED'
    assert explanation['bootPartitionEncryptionDetails']['partitionFileVault2Percent'] == 100
    assert explanation['bootPartitionEncryptionDetails']['partitionName'] == 'C:'


def test_extract_encryption_unprotected():
    enabled, explanation = transform._extract_encryption({'protection_status0': '0'})
    assert enabled is False
    assert explanation['bootPartitionEncryptionDetails']['partitionFileVault2State'] == 'DECRYPTED'
    assert explanation['bootPartitionEncryptionDetails']['partitionFileVault2Percent'] is None


def test_extract_encryption_unknown_status_is_not_enabled():
    """protection_status0 == 2 (WMI 'Unknown') must not be treated as protected."""
    enabled, _ = transform._extract_encryption({'protection_status0': '2'})
    assert enabled is False


def test_extract_model_prefers_computer_system():
    model = transform._extract_model(
        {'CPUType0': 'Intel(R) Core(TM) i7-8650U CPU @ 1.90GHz'},
        {'model0': 'Latitude 5420', 'manufacturer0': 'Dell Inc.'},
    )
    assert model == 'Latitude 5420'


def test_extract_model_falls_back_to_cpu_type_when_table_absent():
    model = transform._extract_model({'CPUType0': 'Intel(R) Core(TM) i7-8650U CPU @ 1.90GHz'}, None)
    assert model == 'Intel(R) Core(TM) i7-8650U CPU @ 1.90GHz'


def test_extract_model_falls_back_when_model0_empty():
    model = transform._extract_model(
        {'cpu_type0': 'AMD Ryzen 7 PRO 4750U'},
        {'model0': '', 'manufacturer0': 'Dell Inc.'},
    )
    assert model == 'AMD Ryzen 7 PRO 4750U'


def test_extract_model_none_when_no_source_available():
    assert transform._extract_model({}, None) is None


def _minimal_features(device):
    """Build a complete-enough features dict for format_for_drata(), varying only device."""
    return {
        'resource_id': 1,
        'device': device,
        'user': {},
        'model': None,
        'mac_address': None,
        'av_enabled': False,
        'av_apps': [],
        'pm_enabled': False,
        'pm_apps': [],
        'au_enabled': False,
        'au_explanation': '',
        'app_list': [],
        'fw_enabled': None,
        'fw_explanation': None,
        'enc_enabled': None,
        'enc_explanation': None,
        'sl_enabled': None,
        'sl_explanation': None,
        'sl_time': None,
        'windows_services': [],
    }


def test_platform_name_and_version_prefer_pascal_case():
    result = transform.format_for_drata(_minimal_features({
        'Operating_System_Name_and0': 'Microsoft Windows 11 Enterprise',
        'Build01': '22631',
    }))
    assert result['platformName'] == 'WINDOWS'
    assert result['platformVersion'] == '22631'


def test_platform_name_and_version_fall_back_to_snake_case():
    """Real Databricks-sourced device rows use snake_case (t_sccm_r_system), never PascalCase."""
    result = transform.format_for_drata(_minimal_features({
        'operating_system_name_and0': 'Microsoft Windows 11 Enterprise',
        'build01': '22631',
        'build_ext': '3593',
    }))
    assert result['platformName'] == 'WINDOWS'
    assert result['platformVersion'] == '22631'


def test_platform_version_falls_back_to_build_ext_when_build01_absent():
    result = transform.format_for_drata(_minimal_features({'build_ext': '3593'}))
    assert result['platformVersion'] == '3593'


def test_platform_name_and_version_unknown_when_no_source_available():
    result = transform.format_for_drata(_minimal_features({}))
    assert result['platformName'] == 'WINDOWS'  # _platform_name's own documented fallback
    assert result['platformVersion'] == 'Unknown'


def test_apply_sandbox_overrides_rewrites_lowercase_domain():
    records = [{'personnelId': 'email:wadei1@nationwide.com'}]
    result = transform.apply_sandbox_overrides(records)
    assert result[0]['personnelId'] == 'email:wadei1@sandbox.nationwide.com'


def test_apply_sandbox_overrides_rewrites_capital_n_domain():
    """Regression: a real @Nationwide.com (capital N) record was left unrewritten by the old
    case-sensitive check, got pushed with the real domain, and 404'd against Drata's sandbox.
    Confirmed 2026-08-18 via a real prod sandbox run (personnelId=email:WADEI1@Nationwide.com)."""
    records = [{'personnelId': 'email:WADEI1@Nationwide.com'}]
    result = transform.apply_sandbox_overrides(records)
    assert result[0]['personnelId'] == 'email:WADEI1@sandbox.nationwide.com'


def test_apply_sandbox_overrides_rewrites_mixed_case_domain():
    records = [{'personnelId': 'email:user@NationWide.Com'}]
    result = transform.apply_sandbox_overrides(records)
    assert result[0]['personnelId'] == 'email:user@sandbox.nationwide.com'


def test_apply_sandbox_overrides_leaves_non_nationwide_domain_untouched():
    records = [{'personnelId': 'email:user@example.com'}]
    result = transform.apply_sandbox_overrides(records)
    assert result[0]['personnelId'] == 'email:user@example.com'


def test_apply_sandbox_overrides_leaves_non_string_personnel_id_untouched():
    records = [{'personnelId': None}, {'personnelId': 12345}]
    result = transform.apply_sandbox_overrides(records)
    assert result[0]['personnelId'] is None
    assert result[1]['personnelId'] == 12345


def test_resolve_personnel_id_prefers_iamdb_mail():
    """2026-08-24: mail (iamdb, authoritative) must win over any SCCM/xlsx field when both
    are present -- iamdb is Nationwide's real identity source, not a copy of it."""
    user = {
        'mail': 'jdoe@nationwide.com',
        'user_principal_name0': 'stale-sccm-copy@nationwide.com',
    }
    assert transform._resolve_personnel_id(user) == 'email:jdoe@nationwide.com'


def test_resolve_personnel_id_falls_back_to_xlsx_fields_without_mail():
    user = {'User_Princiipal_Name0': 'jdoe@nationwide.com'}
    assert transform._resolve_personnel_id(user) == 'email:jdoe@nationwide.com'

    user = {'User_Principal_Name0': 'jdoe@nationwide.com'}
    assert transform._resolve_personnel_id(user) == 'email:jdoe@nationwide.com'

    user = {'user_principal_name0': 'jdoe@nationwide.com'}
    assert transform._resolve_personnel_id(user) == 'email:jdoe@nationwide.com'


def test_resolve_personnel_id_none_when_no_valid_email_anywhere():
    assert transform._resolve_personnel_id({}) is None
    assert transform._resolve_personnel_id({'mail': 'not-an-email'}) is None


def test_ci_get_matches_exact_case():
    assert transform._ci_get({'auoptions0': '4'}, 'auoptions0') == '4'


def test_ci_get_matches_different_case():
    assert transform._ci_get({'AUOptions0': '4'}, 'auoptions0') == '4'


def test_ci_get_missing_key_returns_none():
    assert transform._ci_get({'SomeOtherColumn': '4'}, 'auoptions0') is None


def test_auto_update_matches_auoptions0_regardless_of_case():
    enabled, explanation = transform._auto_update({'AUOptions0': '4'})
    assert enabled is True
    assert explanation == 'Auto download and install'


def test_auto_update_lowercase_key_still_works():
    enabled, explanation = transform._auto_update({'auoptions0': '4'})
    assert enabled is True


def test_auto_update_missing_key_is_unknown_not_silently_disabled():
    enabled, explanation = transform._auto_update({})
    assert enabled is False
    assert explanation == 'Unknown'


def test_auto_update_notify_before_install_counts_as_compliant():
    """'3' still guarantees updates download automatically -- it only avoids an unattended
    install interrupting someone's active work, so it must pass alongside '4'."""
    enabled, explanation = transform._auto_update({'auoptions0': '3'})
    assert enabled is True
    assert explanation == 'Auto download, notify before install'


def test_auto_update_notify_before_download_is_not_compliant():
    enabled, _ = transform._auto_update({'auoptions0': '2'})
    assert enabled is False


def test_auto_update_disabled_is_not_compliant():
    enabled, _ = transform._auto_update({'auoptions0': '1'})
    assert enabled is False


def test_extract_screen_lock_none_when_table_absent():
    assert transform._extract_screen_lock(None) == (None, None, None)


def test_extract_screen_lock_enabled_converts_seconds_to_minutes():
    row = {'screen_saver_active0': 1, 'screen_saver_secure0': '1', 'screen_saver_timeout0': '900'}
    enabled, explanation, wait = transform._extract_screen_lock(row)
    assert enabled is True
    assert wait == 15  # 900 seconds -> 15 minutes
    assert explanation == 'ScreenLock delay is 15 minutes'


def test_extract_screen_lock_disabled_when_not_secure():
    row = {'screen_saver_active0': 1, 'screen_saver_secure0': '0', 'screen_saver_timeout0': '600'}
    enabled, _, _ = transform._extract_screen_lock(row)
    assert enabled is False


def test_extract_screen_lock_disabled_when_not_active():
    row = {'screen_saver_active0': 0, 'screen_saver_secure0': '1', 'screen_saver_timeout0': '600'}
    enabled, explanation, wait = transform._extract_screen_lock(row)
    assert enabled is False
    assert wait == 10
