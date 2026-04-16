from __future__ import annotations

import contextlib
import datetime
from types import SimpleNamespace
from textwrap import dedent

import aiohttp
import aiostream
import pytest
from aioresponses import aioresponses

from tests import EVENT_TEMPLATE
from tests import TASK_TEMPLATE
from tests import VCARD_TEMPLATE
from tests.storage import format_item
from vdirsyncer import exceptions
from vdirsyncer.storage.dav import CalDiscover
from vdirsyncer.storage.dav import CalDAVStorage

from . import DAVStorageTests
from . import dav_server


class TestCalDAVStorage(DAVStorageTests):
    storage_class = CalDAVStorage

    @pytest.fixture(params=["VTODO", "VEVENT"])
    def item_type(self, request):
        return request.param

    @pytest.mark.asyncio
    async def test_doesnt_accept_vcard(self, item_type, get_storage_args):
        s = self.storage_class(item_types=(item_type,), **await get_storage_args())

        # Most storages hard-fail, but xandikos doesn't.
        with contextlib.suppress(exceptions.Error, aiohttp.ClientResponseError):
            await s.upload(format_item(VCARD_TEMPLATE))

        assert not await aiostream.stream.list(s.list())

    # The `arg` param is not named `item_types` because that would hit
    # https://bitbucket.org/pytest-dev/pytest/issue/745/
    @pytest.mark.parametrize(
        ("arg", "calls_num"),
        [
            (("VTODO",), 1),
            (("VEVENT",), 1),
            (("VTODO", "VEVENT"), 2),
            (("VTODO", "VEVENT", "VJOURNAL"), 3),
            ((), 1),
        ],
    )
    @pytest.mark.xfail(dav_server == "baikal", reason="Baikal returns 500.")
    @pytest.mark.asyncio
    async def test_item_types_performance(
        self, get_storage_args, arg, calls_num, monkeypatch
    ):
        s = self.storage_class(item_types=arg, **await get_storage_args())
        old_parse = s._parse_prop_responses
        calls = []

        def new_parse(*a, **kw):
            calls.append(None)
            return old_parse(*a, **kw)

        monkeypatch.setattr(s, "_parse_prop_responses", new_parse)
        await aiostream.stream.list(s.list())
        assert len(calls) == calls_num

    @pytest.mark.xfail(
        dav_server == "radicale", reason="Radicale doesn't support timeranges."
    )
    @pytest.mark.asyncio
    async def test_timerange_correctness(self, get_storage_args):
        start_date = datetime.datetime(2013, 9, 10)
        end_date = datetime.datetime(2013, 9, 13)
        s = self.storage_class(
            start_date=start_date, end_date=end_date, **await get_storage_args()
        )

        too_old_item = format_item(
            dedent(
                """
            BEGIN:VCALENDAR
            VERSION:2.0
            PRODID:-//hacksw/handcal//NONSGML v1.0//EN
            BEGIN:VEVENT
            DTSTART:19970714T170000Z
            DTEND:19970715T035959Z
            SUMMARY:Bastille Day Party
            X-SOMETHING:{r}
            UID:{r}
            END:VEVENT
            END:VCALENDAR
            """
            ).strip()
        )

        too_new_item = format_item(
            dedent(
                """
            BEGIN:VCALENDAR
            VERSION:2.0
            PRODID:-//hacksw/handcal//NONSGML v1.0//EN
            BEGIN:VEVENT
            DTSTART:20150714T170000Z
            DTEND:20150715T035959Z
            SUMMARY:Another Bastille Day Party
            X-SOMETHING:{r}
            UID:{r}
            END:VEVENT
            END:VCALENDAR
            """
            ).strip()
        )

        good_item = format_item(
            dedent(
                """
            BEGIN:VCALENDAR
            VERSION:2.0
            PRODID:-//hacksw/handcal//NONSGML v1.0//EN
            BEGIN:VEVENT
            DTSTART:20130911T170000Z
            DTEND:20130912T035959Z
            SUMMARY:What's with all these Bastille Day Partys
            X-SOMETHING:{r}
            UID:{r}
            END:VEVENT
            END:VCALENDAR
            """
            ).strip()
        )

        await s.upload(too_old_item)
        await s.upload(too_new_item)
        expected_href, _ = await s.upload(good_item)

        ((actual_href, _),) = await aiostream.stream.list(s.list())
        assert actual_href == expected_href

    @pytest.mark.asyncio
    async def test_invalid_resource(self, monkeypatch, get_storage_args):
        args = await get_storage_args(collection=None)

        with aioresponses() as m:
            m.add(args["url"], method="PROPFIND", status=200, body="Hello world")

            with pytest.raises(ValueError):
                s = self.storage_class(**args)
                await aiostream.stream.list(s.list())

        assert len(m.requests) == 1

    @pytest.mark.asyncio
    async def test_create_collection_preserves_item_types(self, monkeypatch):
        calls = []

        class FakeSession:
            def get_default_headers(self):
                return {"Content-Type": "application/xml; charset=UTF-8"}

            async def request(self, method, url, **kwargs):
                calls.append((method, url, kwargs))
                return SimpleNamespace(url=url)

        async def empty_discover(self):
            if False:
                yield None

        async def fake_find_home(self):
            return "https://dav.test/home/"

        def fake_init_and_remaining_args(**kwargs):
            kwargs = dict(kwargs)
            kwargs.pop("connector", None)
            return FakeSession(), kwargs

        monkeypatch.setattr(CalDiscover, "discover", empty_discover)
        monkeypatch.setattr(CalDiscover, "find_home", fake_find_home)
        monkeypatch.setattr(
            self.storage_class.session_class,
            "init_and_remaining_args",
            fake_init_and_remaining_args,
        )

        created = await self.storage_class.create_collection(
            url="https://dav.test/calendar/",
            username="user",
            password="pass",
            collection="tasks",
            item_types=["VTODO"],
            connector=None,
        )

        assert created["collection"] == "tasks"
        assert created["url"] == "https://dav.test/home/tasks"

        ((method, url, kwargs),) = calls
        assert method == "MKCALENDAR"
        assert url == "https://dav.test/home/tasks"
        payload = kwargs["data"].decode("utf-8")
        assert '<C:comp name="VTODO"/>' in payload
        assert '<C:comp name="VEVENT"/>' not in payload

    @pytest.mark.asyncio
    async def test_create_collection_default_components(self, monkeypatch):
        calls = []

        class FakeSession:
            def get_default_headers(self):
                return {"Content-Type": "application/xml; charset=UTF-8"}

            async def request(self, method, url, **kwargs):
                calls.append((method, url, kwargs))
                return SimpleNamespace(url=url)

        async def empty_discover(self):
            if False:
                yield None

        async def fake_find_home(self):
            return "https://dav.test/home/"

        def fake_init_and_remaining_args(**kwargs):
            kwargs = dict(kwargs)
            kwargs.pop("connector", None)
            return FakeSession(), kwargs

        monkeypatch.setattr(CalDiscover, "discover", empty_discover)
        monkeypatch.setattr(CalDiscover, "find_home", fake_find_home)
        monkeypatch.setattr(
            self.storage_class.session_class,
            "init_and_remaining_args",
            fake_init_and_remaining_args,
        )

        created = await self.storage_class.create_collection(
            url="https://dav.test/calendar/",
            username="user",
            password="pass",
            collection="calendar",
            connector=None,
        )

        assert created["url"] == "https://dav.test/home/calendar"

        ((_, _, kwargs),) = calls
        payload = kwargs["data"].decode("utf-8")
        assert '<C:comp name="VEVENT"/>' in payload
        assert '<C:comp name="VTODO"/>' in payload

    @pytest.mark.skipif(dav_server == "icloud", reason="iCloud only accepts VEVENT")
    @pytest.mark.skipif(
        dav_server == "fastmail", reason="Fastmail has non-standard hadling of VTODOs."
    )
    @pytest.mark.xfail(dav_server == "baikal", reason="Baikal returns 500.")
    @pytest.mark.asyncio
    async def test_item_types_general(self, s):
        event = (await s.upload(format_item(EVENT_TEMPLATE)))[0]
        task = (await s.upload(format_item(TASK_TEMPLATE)))[0]
        s.item_types = ("VTODO", "VEVENT")

        async def hrefs():
            return {href async for href, etag in s.list()}

        assert await hrefs() == {event, task}
        s.item_types = ("VTODO",)
        assert await hrefs() == {task}
        s.item_types = ("VEVENT",)
        assert await hrefs() == {event}
        s.item_types = ()
        assert await hrefs() == {event, task}
