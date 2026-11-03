from __future__ import annotations

from pathlib import Path

from vdirsyncer.exceptions import UserError
from vdirsyncer.storage.filesystem import FilesystemStorage

from . import sync_logger
from .status import ItemMetadata
from .status import SubStatus


def _file_href(source, filename):
    root = Path(source.path).resolve()
    path = Path(filename)
    if path.is_absolute() or len(path.parts) != 1 or path.name != filename:
        raise UserError("Push files must be filenames relative to the collection.")
    if (root / path).resolve().parent != root:
        raise UserError("Push files must not point outside the collection.")
    if not filename.endswith(source.fileext) or filename.endswith(source.fileignoreext):
        raise UserError("Push file does not match the filesystem storage's extension.")
    return filename


def _validate_item(item):
    parsed = item.parsed
    if parsed is None or parsed.name != "VCALENDAR":
        raise UserError("Push requires an iCalendar resource with a UID.")
    components = [c for c in parsed.subcomponents if c.name != "VTIMEZONE"]
    if (
        not components
        or not item.uid
        or any(
            c.name not in ("VEVENT", "VTODO", "VJOURNAL")
            or c.get("UID", "").strip() != item.uid
            for c in components
        )
    ):
        raise UserError("Push requires a single UID, including recurrence exceptions.")


async def push(
    source, destination, status, filenames, *, source_side="a", error_callback=None
):
    """Pushes selected local resources without listing either storage.

    Each successful item is committed independently. Conditional destination writes
    use the last synchronized ETag; conflicts are reported, never overwritten.
    """
    if not isinstance(source, FilesystemStorage):
        raise UserError("Push requires a filesystem source.")
    if destination.read_only:
        raise UserError("Push destination is read-only.")
    if source_side not in ("a", "b"):
        raise ValueError("Invalid source side")

    local = SubStatus(status, source_side)
    remote = SubStatus(status, "b" if source_side == "a" else "a")
    for filename in dict.fromkeys(filenames):
        try:
            href = _file_href(source, filename)
            # Hold the same exclusive status lock as full sync, including during
            # the write, so neither operation can use the other's stale ETag.
            with status.transaction(preserve=True):
                async with destination.at_once():
                    await _push_item(source, destination, local, remote, href)
        except Exception as error:
            if error_callback is None:
                raise
            error_callback(error)


async def _push_item(source, destination, local, remote, href):
    item, etag = await source.get(href)
    _validate_item(item)
    old_ident, _ = local.get_by_href(href)
    if old_ident is not None and old_ident != item.ident:
        raise UserError("The UID of a tracked file changed; run a full sync.")
    old_local = local.get(item.ident)
    old_remote = remote.get(item.ident)
    if old_local is not None and old_local.href != href:
        raise UserError("This UID is already tracked under another filename.")

    local_props = ItemMetadata(href=href, hash=item.hash, etag=etag)
    if old_local is not None and old_remote is not None:
        if old_local.hash == item.hash:
            local.update_ident(item.ident, local_props)
            return
        if not old_remote.href or not old_remote.etag:
            raise UserError("No remote ETag available; run a full sync first.")
        sync_logger.info(f"Copying (updating) item {item.ident} to {destination}")
        remote_etag = await destination.update(old_remote.href, item, old_remote.etag)
        remote_href = old_remote.href
    elif old_local is None and old_remote is None:
        sync_logger.info(f"Copying (uploading) item {item.ident} to {destination}")
        remote_href, remote_etag = await destination.upload(item)
    else:
        raise UserError("Incomplete item status; run a full sync first.")

    remote_props = ItemMetadata(href=remote_href, hash=item.hash, etag=remote_etag)
    if old_local is None:
        local.insert_ident(item.ident, local_props)
        remote.insert_ident(item.ident, remote_props)
    else:
        local.update_ident(item.ident, local_props)
        remote.update_ident(item.ident, remote_props)
