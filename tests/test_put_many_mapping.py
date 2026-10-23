import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapTypeError


@pytest.mark.parametrize("bad", [None, [("a", 1)], (pair for pair in [("a", 1)]), "ab"])
def test_put_many_rejects_non_mappings(bad):
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError, match="requires a mapping"):
        cm.put_many(bad)

    assert len(cm) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, [("a", 1)], "ab"])
async def test_async_put_many_rejects_non_mappings(bad):
    cm = AsyncChronoMap()

    with pytest.raises(ChronoMapTypeError, match="requires a mapping"):
        await cm.put_many(bad)

    assert await cm.size() == 0
