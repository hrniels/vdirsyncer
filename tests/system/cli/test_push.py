from __future__ import annotations

import threading
from textwrap import dedent
from wsgiref.simple_server import make_server

import pytest
import requests

from tests.unit.sync.test_push import event


def configure(runner, tmpdir, remote=None):
    tmpdir.ensure_dir("local")
    if remote is None:
        tmpdir.ensure_dir("remote")
        remote_config = (
            f'type = "filesystem"\npath = "{tmpdir}/remote"\nfileext = ".ics"'
        )
    else:
        remote_config = (
            f'type = "caldav"\nurl = "{remote}"\nusername = ""\npassword = ""'
        )
    runner.write_with_general(
        dedent(f"""
        [pair test]
        a = "local"
        b = "remote"
        collections = null

        [storage local]
        type = "filesystem"
        path = "{tmpdir}/local"
        fileext = ".ics"

        [storage remote]
    """)
        + remote_config
        + "\n"
    )


def invoke(runner, *args):
    result = runner.invoke(list(args))
    assert result.exit_code == 0, result.output
    return result


def test_push_cli(tmpdir, runner):
    configure(runner, tmpdir)
    invoke(runner, "discover")
    tmpdir.join("local/selected.ics").write(event())
    tmpdir.join("local/other.ics").write(event("other"))
    result = runner.invoke(["push", "test", "selected.ics"])
    assert result.exit_code != 0
    assert "--from" in result.output
    invoke(runner, "push", "--from", "a", "test", "selected.ics")
    assert tmpdir.join("remote/selected.ics").exists()
    assert not tmpdir.join("remote/other.ics").exists()
    tmpdir.join("local/selected.ics").write(event(summary="updated"))
    invoke(runner, "push", "--from", "a", "test", "selected.ics")
    assert "updated" in tmpdir.join("remote/selected.ics").read()
    result = invoke(runner, "sync")
    assert "item selected" not in result.output


def test_push_requires_discovery_and_reports_partial_failure(tmpdir, runner):
    configure(runner, tmpdir)
    tmpdir.join("local/selected.ics").write(event())
    result = runner.invoke(["push", "--from", "a", "test", "selected.ics"])
    assert result.exit_code != 0
    assert "discover" in result.output
    invoke(runner, "discover")
    result = runner.invoke(
        ["push", "--from", "a", "test", "missing.ics", "selected.ics"]
    )
    assert result.exit_code != 0
    assert tmpdir.join("remote/selected.ics").exists()


def test_push_selects_exactly_one_calendar(tmpdir, runner):
    configure(runner, tmpdir)
    runner.cfg.write(
        runner.cfg.read().replace(
            "collections = null", 'collections = ["work", "home"]'
        )
    )
    for side in ("local", "remote"):
        for calendar in ("work", "home"):
            tmpdir.ensure_dir(side, calendar)
    invoke(runner, "discover")
    for calendar in ("work", "home"):
        tmpdir.join(f"local/{calendar}/selected.ics").write(event())
    result = runner.invoke(["push", "--from", "a", "test", "selected.ics"])
    assert result.exit_code != 0
    assert "exactly one" in result.output
    invoke(runner, "push", "--from", "a", "test/work", "selected.ics")
    assert tmpdir.join("remote/work/selected.ics").exists()
    assert not tmpdir.join("remote/home/selected.ics").exists()


def test_push_cli_from_b(tmpdir, runner):
    configure(runner, tmpdir)
    invoke(runner, "discover")
    tmpdir.join("remote/selected.ics").write(event())
    invoke(runner, "push", "--from", "b", "test", "selected.ics")
    assert tmpdir.join("local/selected.ics").exists()
    assert "item selected" not in invoke(runner, "sync").output


@pytest.fixture
def radicale_url(tmp_path):
    # Keep the real-server test optional, without requiring Docker or Eventix.
    radicale = pytest.importorskip("radicale")
    config = radicale.config.load([])
    config.update(
        {
            "auth": {"type": "none"},
            "rights": {"type": "owner_only"},
            "storage": {"filesystem_folder": str(tmp_path / "radicale")},
        }
    )
    application = radicale.Application(config)
    recorded = []

    def recording_app(environ, start_response):
        recorded.append((environ["REQUEST_METHOD"], environ["PATH_INFO"]))
        return application(environ, start_response)

    with make_server("127.0.0.1", 0, recording_app) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/user/calendar/"
            response = requests.request(
                "MKCOL", url.rsplit("calendar/", 1)[0], timeout=5
            )
            assert response.status_code == 201, response.text
            response = requests.request("MKCALENDAR", url, timeout=5)
            assert response.status_code == 201, response.text
            yield url, recorded
        finally:
            server.shutdown()
            thread.join(timeout=5)


def test_push_cli_real_caldav(tmpdir, runner, radicale_url):
    url, requests_seen = radicale_url
    configure(runner, tmpdir, remote=url)
    invoke(runner, "discover")
    tmpdir.join("local/selected.ics").write(event())
    tmpdir.join("local/other.ics").write(event("other"))
    requests_seen.clear()
    invoke(runner, "push", "test", "selected.ics")
    tmpdir.join("local/selected.ics").write(event(summary="updated"))
    invoke(runner, "push", "test", "selected.ics")
    assert requests_seen == [("PUT", "/user/calendar/selected.ics")] * 2
    response = requests.get(url + "selected.ics", timeout=5)
    assert response.status_code == 200
    assert "updated" in response.text
    assert requests.get(url + "other.ics", timeout=5).status_code == 404

    # A remote-only resource is not pulled by push.
    response = requests.put(
        url + "remote.ics",
        data=event("remote"),
        headers={"Content-Type": "text/calendar"},
        timeout=5,
    )
    assert response.status_code == 201
    requests_seen.clear()
    invoke(runner, "push", "test", "selected.ics")
    assert requests_seen == []
    assert not tmpdir.join("local/remote.ics").exists()
    result = invoke(runner, "sync")
    assert "item selected" not in result.output
    assert tmpdir.join("local/remote.ics").exists()

    # An outdated ETag must not overwrite an edit made by another client.
    response = requests.put(
        url + "selected.ics",
        data=event(summary="remote edit"),
        headers={"Content-Type": "text/calendar"},
        timeout=5,
    )
    assert response.status_code == 204
    tmpdir.join("local/selected.ics").write(event(summary="local edit"))
    requests_seen.clear()
    result = runner.invoke(["push", "test", "selected.ics"])
    assert result.exit_code != 0
    assert requests_seen == [("PUT", "/user/calendar/selected.ics")]
    assert "remote edit" in requests.get(url + "selected.ics", timeout=5).text
