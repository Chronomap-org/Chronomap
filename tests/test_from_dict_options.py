import pytest

from chronomap import ChronoMap
from chronomap.exceptions import ChronoMapValueError

PAYLOAD = {
    "store": {"key": [[1.0, "value"]]},
    "ttl": {},
    "snapshot_time": None,
    "max_history": 2,
}


def test_from_dict_uses_payload_max_history_by_default():
    cm = ChronoMap.from_dict(PAYLOAD)

    assert cm._max_history == 2


def test_from_dict_explicit_max_history_overrides_payload():
    cm = ChronoMap.from_dict(PAYLOAD, max_history=5)

    assert cm._max_history == 5


def test_from_dict_explicit_none_disables_payload_limit():
    cm = ChronoMap.from_dict(PAYLOAD, max_history=None)

    assert cm._max_history is None


def test_from_dict_validates_explicit_max_history():
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict(PAYLOAD, max_history=0)
