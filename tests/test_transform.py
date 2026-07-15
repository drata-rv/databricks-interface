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
