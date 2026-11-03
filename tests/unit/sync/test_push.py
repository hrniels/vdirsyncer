from __future__ import annotations

import contextlib
import sqlite3

import pytest

from vdirsyncer.exceptions import AlreadyExistingError
from vdirsyncer.exceptions import NotFoundError
from vdirsyncer.exceptions import UserError
from vdirsyncer.exceptions import WrongEtagError
from vdirsyncer.storage.filesystem import FilesystemStorage
from vdirsyncer.storage.memory import MemoryStorage
from vdirsyncer.sync import sync
from vdirsyncer.sync.push import push
from vdirsyncer.sync.status import SqliteStatus
from vdirsyncer.vobject import Item


def event(uid="selected", summary="original"):
    return (
        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//push test//EN\r\n"
        f"BEGIN:VEVENT\r\nUID:{uid}\r\nSUMMARY:{summary}\r\n"
        "DTSTART:20261103T100000Z\r\nDTEND:20261103T110000Z\r\n"
        "END:VEVENT\r\nEND:VCALENDAR\r\n"
    )


@pytest.fixture
def pair(tmp_path):
    source = FilesystemStorage(path=str(tmp_path), fileext=".ics")
    destination = MemoryStorage()
    with contextlib.closing(SqliteStatus(str(tmp_path / "status"))) as status:
        yield source, destination, status


def write(source, filename="selected.ics", **kwargs):
    from pathlib import Path

    Path(source.path, filename).write_text(event(**kwargs))


@pytest.mark.asyncio
async def test_push_preserves_unselected_changes_and_full_sync(pair, monkeypatch):
    source, destination, status = pair
    write(source, "other.ics", uid="other")
    await sync(source, destination, status)
    old_other = dict(status.to_legacy_status())["other"]
    write(source, "other.ics", uid="other", summary="local pending")
    remote_only = Item(event("remote"))
    await destination.upload(remote_only)
    write(source)

    def no_listing(*args, **kwargs):
        raise AssertionError("Push must not list either storage")

    with monkeypatch.context() as patch:
        patch.setattr(source, "list", no_listing)
        patch.setattr(destination, "list", no_listing)
        await push(source, destination, status, ["selected.ics"])
        write(source, summary="updated")
        await push(source, destination, status, ["selected.ics"])

    assert dict(status.to_legacy_status())["other"] == old_other
    assert set(status.iter_old()) == {"other", "selected"}
    selected_etag = status.get_b("selected").etag
    await sync(source, destination, status)
    assert status.get_b("selected").etag == selected_etag
    assert set(status.iter_old()) == {"other", "selected", "remote"}
    assert "updated" in (await destination.get(status.get_b("selected").href))[0].raw


@pytest.mark.asyncio
async def test_stale_etag_leaves_status_and_server_untouched(pair):
    source, destination, status = pair
    write(source)
    await push(source, destination, status, ["selected.ics"])
    before = dict(status.to_legacy_status())
    meta = status.get_b("selected")
    await destination.update(meta.href, Item(event(summary="remote change")), meta.etag)
    write(source, summary="local change")
    with pytest.raises(WrongEtagError):
        await push(source, destination, status, ["selected.ics"])
    assert dict(status.to_legacy_status()) == before
    assert "remote change" in (await destination.get(meta.href))[0].raw


@pytest.mark.asyncio
async def test_partial_success_and_duplicate_arguments(pair):
    source, destination, status = pair
    write(source)
    errors = []
    await push(
        source,
        destination,
        status,
        ["missing.ics", "selected.ics", "selected.ics"],
        error_callback=errors.append,
    )
    assert len(errors) == 1
    assert list(status.iter_old()) == ["selected"]
    assert len(destination.items) == 1


@pytest.mark.parametrize(
    "filename", ["../escape.ics", "/absolute.ics", "sub/event.ics", "event.txt"]
)
@pytest.mark.asyncio
async def test_invalid_paths(pair, filename):
    source, destination, status = pair
    with pytest.raises(UserError):
        await push(source, destination, status, [filename])
    assert not list(status.iter_old())


@pytest.mark.asyncio
async def test_external_symlink(pair, tmp_path):
    source, destination, status = pair
    (tmp_path / "selected.ics").symlink_to(tmp_path.parent / "outside.ics")
    with pytest.raises(UserError):
        await push(source, destination, status, ["selected.ics"])


@pytest.mark.asyncio
async def test_changed_uid_and_duplicate_uid(pair):
    source, destination, status = pair
    write(source)
    await push(source, destination, status, ["selected.ics"])
    write(source, uid="different")
    with pytest.raises(UserError, match="UID.*changed"):
        await push(source, destination, status, ["selected.ics"])
    write(source, "duplicate.ics")
    with pytest.raises(UserError, match="another filename"):
        await push(source, destination, status, ["duplicate.ics"])


@pytest.mark.asyncio
async def test_read_only_destination(pair):
    source, destination, status = pair
    destination.read_only = True
    write(source)
    with pytest.raises(UserError, match="read-only"):
        await push(source, destination, status, ["selected.ics"])
    assert not list(status.iter_old())


@pytest.mark.asyncio
async def test_missing_etag_requires_full_sync(pair, monkeypatch):
    source, destination, status = pair
    upload = destination.upload

    async def upload_without_etag(item):
        href, _ = await upload(item)
        return href, None

    monkeypatch.setattr(destination, "upload", upload_without_etag)
    write(source)
    await push(source, destination, status, ["selected.ics"])
    write(source, summary="updated")
    with pytest.raises(UserError, match="ETag"):
        await push(source, destination, status, ["selected.ics"])
    await sync(source, destination, status)
    assert status.get_b("selected").etag is not None


@pytest.mark.asyncio
async def test_source_b(pair):
    source, destination, status = pair
    write(source)
    await push(source, destination, status, ["selected.ics"], source_side="b")
    write(source, summary="updated")
    await push(source, destination, status, ["selected.ics"], source_side="b")
    await sync(destination, source, status)
    assert status.get_b("selected").href == "selected.ics"


def test_preserving_transaction_locks_out_full_sync(tmp_path):
    path = str(tmp_path / "status")
    first = SqliteStatus(path)
    second = SqliteStatus(path)
    second._c.execute("PRAGMA busy_timeout = 0")
    try:
        with (
            first.transaction(preserve=True),
            pytest.raises(sqlite3.OperationalError, match="locked"),
            second.transaction(),
        ):
            pytest.fail("Concurrent full sync acquired the lock")
        with second.transaction():
            pass
    finally:
        first.close()
        second.close()


@pytest.mark.asyncio
async def test_remote_deletion_is_not_recreated(pair):
    source, destination, status = pair
    write(source)
    await push(source, destination, status, ["selected.ics"])
    before = dict(status.to_legacy_status())
    meta = status.get_b("selected")
    await destination.delete(meta.href, meta.etag)
    write(source, summary="updated")
    with pytest.raises(NotFoundError):
        await push(source, destination, status, ["selected.ics"])
    assert not destination.items
    assert dict(status.to_legacy_status()) == before


@pytest.mark.asyncio
async def test_new_item_collision_does_not_overwrite(pair):
    source, destination, status = pair
    await destination.upload(Item(event(summary="remote")))
    write(source)
    with pytest.raises(AlreadyExistingError):
        await push(source, destination, status, ["selected.ics"])
    assert not list(status.iter_old())
    assert "remote" in (await destination.get("selected"))[0].raw


@pytest.mark.parametrize(
    "raw",
    [
        "invalid",
        event().replace("UID:selected\r\n", ""),
        event().replace("END:VCALENDAR", event("another")),
        event().replace(
            "END:VCALENDAR",
            "BEGIN:VEVENT\r\nUID:another\r\nEND:VEVENT\r\nEND:VCALENDAR",
        ),
    ],
)
@pytest.mark.asyncio
async def test_invalid_or_multiple_uid_resources(pair, raw, tmp_path):
    source, destination, status = pair
    (tmp_path / "selected.ics").write_text(raw)
    with pytest.raises(UserError):
        await push(source, destination, status, ["selected.ics"])
    assert not destination.items
    assert not list(status.iter_old())


@pytest.mark.asyncio
async def test_recurring_resource_is_pushed_as_one_item(pair, tmp_path):
    source, destination, status = pair
    exception = (
        "BEGIN:VEVENT\r\nUID:selected\r\nRECURRENCE-ID:20261110T100000Z\r\n"
        "DTSTART:20261110T120000Z\r\nEND:VEVENT\r\n"
    )
    raw = event().replace("END:VCALENDAR", exception + "END:VCALENDAR")
    (tmp_path / "selected.ics").write_text(raw)
    await push(source, destination, status, ["selected.ics"])
    assert len(destination.items) == 1
    assert "RECURRENCE-ID" in (await destination.get("selected"))[0].raw


@pytest.mark.asyncio
async def test_normalized_upload_without_etag_is_reconciled(pair, monkeypatch):
    source, destination, status = pair
    upload = destination.upload

    async def normalized_upload(item):
        href, _ = await upload(Item(item.raw.replace("original", "normalized")))
        return href, None

    monkeypatch.setattr(destination, "upload", normalized_upload)
    write(source)
    await push(source, destination, status, ["selected.ics"])
    await sync(source, destination, status)
    item, _ = await source.get("selected.ics")
    assert "normalized" in item.raw
    assert status.get_a("selected").hash == status.get_b("selected").hash
    before = dict(status.to_legacy_status())
    await push(source, destination, status, ["selected.ics"])
    assert dict(status.to_legacy_status()) == before
