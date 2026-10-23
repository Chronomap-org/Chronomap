import pytest

from chronomap import AsyncChronoMap
from chronomap.exceptions import ChronoMapValueError

PAYLOAD = {
    "store": {"key": [[1.0, "value"]]},
    "ttl": {},
    "snapshot_time": None,
    "max_history": 2,
}


def test_async_from_dict_uses_payload_defaults():
    cm = AsyncChronoMap.from_dict(PAYLOAD)

    assert cm._max_history == 2
    assert cm._enable_ttl_cleanup is False


def test_async_from_dict_explicit_max_history_overrides_payload():
    cm = AsyncChronoMap.from_dict(PAYLOAD, max_history=5)

    assert cm._max_history == 5


def test_async_from_dict_explicit_none_disables_payload_limit():
    cm = AsyncChronoMap.from_dict(PAYLOAD, max_history=None)

    assert cm._max_history is None


def test_async_from_dict_accepts_enable_ttl_cleanup():
    cm = AsyncChronoMap.from_dict(PAYLOAD, enable_ttl_cleanup=True)

    assert cm._enable_ttl_cleanup is True
    assert cm._cleanup_task is None


def test_async_from_dict_validates_explicit_max_history():
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict(PAYLOAD, max_history=0)


@pytest.mark.asyncio
async def test_async_load_json_forwards_options(tmp_path):
    path = tmp_path / "state.json"
    source = AsyncChronoMap(max_history=2)
    await source.put("key", "value", timestamp=1)
    await source.save_json(path)

    restored = await AsyncChronoMap.load_json(path, max_history=5, enable_ttl_cleanup=True)

    assert restored._max_history == 5
    assert restored._enable_ttl_cleanup is True
    assert await restored.latest() == {"key": "value"}
