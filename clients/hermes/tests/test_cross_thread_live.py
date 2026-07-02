"""End-to-end live proof of structural cross-thread isolation.

Drives the real assembled receive path + connection pool — ``CommyAdapter``
+ ``TopicConnectionManager`` + the real ``McpTopicTransport`` spawning
``bun packages/mcp/server.ts`` — against the real Zulip realm. No mocks.

The property under proof is that per-topic isolation is structural, not
conventional: a per-topic identity never receives another thread's frames
unless it subscribes, because the substrate narrow (``narrow-set.ts``) simply
does not match them — not because an adapter withholds them.

The scenario models one Hermes pod (``owner``) that owns thread-A. A separate
``poster`` identity drives thread-B and thread-C traffic:

* **AC1** — owner, subscribed ``thread:<ch>/A,mentions``, is mentioned in
  thread-B. It receives the *mention* frame (the ``mentions`` narrow matches)
  but not the plain thread-B message posted alongside it (no narrow matches).
* **AC2a** — owner *chooses to subscribe* thread-B by calling
  ``ensure_topic_connection(<ch>, B)`` (the pool's own API — "its context now
  spans both, by its own choice"). A subsequent plain thread-B message now
  reaches it.
* **AC2b** — a third pod that owns thread-C and is never mentioned stays fully
  isolated from thread-B.

Absence is proven by ordering fences, not bare sleeps: a positive-control
message that must arrive is posted after the message that must not, so once the
control is observed the excluded message has had at least as long to arrive.
Markers are run-unique so persistent-mode catch-up replay of prior runs is
filtered out, and catch-up is disabled (window 0) so a fresh subscribe never
replays history into the absence window.

Live-only: gated on the realm env below and marked ``live`` so the default
``pytest`` run (``scripts/test.sh``) skips it; run it via ``scripts/test-live.sh``.

Required env (suite skips silently if any is unset):
* ``ZULIP_SITE`` / ``ZULIP_MINTER_EMAIL`` / ``ZULIP_MINTER_API_KEY``
* ``ZULIP_LIVE_CHANNEL_NAME`` — a channel the minter already streams
* ``COMMY_SERVER_DIR`` (optional) — commy checkout the server runs
  from; defaults to this repo root. Must have ``node_modules`` (``bun install``).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
import uuid
from pathlib import Path
from typing import Optional

import httpx
import pytest
from gateway.config import PlatformConfig
from gateway.platforms.base import MessageEvent
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

from commy import register
from commy.adapter import CommyAdapter
from commy.connection import SpawnConfig
from commy.naming import deterministic_bot_name
from commy.transport import make_manager

# Fixed topic names keep the per-topic bot identities stable across runs (low
# realm churn — the substrate reuses the same user_id by name), while the
# per-run markers below keep each run's traffic unambiguous.
TOPIC_A = "a7j6-owner-a"
TOPIC_B = "a7j6-peer-b"
TOPIC_C = "a7j6-third-c"

# Pace realm-touching calls; the realm rate-limits per user (the minter), and
# every per-topic pod long-polls /events as that one user.
POD_PACE_SECONDS = 1.5
# Bounded wait for a live frame to traverse realm -> long-poll -> pump -> sink.
ARRIVAL_TIMEOUT_SECONDS = 30.0
# Readiness handshake: a Zulip event queue only captures events posted AFTER it
# registers, and registration happens asynchronously inside the forked pump with
# no signal we can await. So we post a warm-up into the pod's OWN topic and
# re-post until the pod observes it — at which point the queue is provably live.
WARMUP_ATTEMPTS = 8
WARMUP_GAP_SECONDS = 2.5
# Grace after a positive control lands, before asserting an excluded message
# is absent — gives a (wrongly) delivered frame room to show up and fail loud.
ABSENCE_GRACE_SECONDS = 2.0
# Poster resilience to a transient shared-minter 429: honor Zulip's full
# Retry-After (the GCRA drain time) rather than capping it.
POST_RETRY_ATTEMPTS = 6
POST_RETRY_BACKOFF_SECONDS = 3.0


def _read_env() -> Optional[dict[str, str]]:
    required = {
        "site": os.environ.get("ZULIP_SITE"),
        "minter_email": os.environ.get("ZULIP_MINTER_EMAIL"),
        "minter_api_key": os.environ.get("ZULIP_MINTER_API_KEY"),
        "channel": os.environ.get("ZULIP_LIVE_CHANNEL_NAME"),
    }
    if any(not value for value in required.values()):
        return None
    return {key: value for key, value in required.items() if value is not None}


_ENV = _read_env()

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        _ENV is None,
        reason="live realm env (ZULIP_SITE / ZULIP_MINTER_* / ZULIP_LIVE_CHANNEL_NAME) not set",
    ),
]


@pytest.fixture(autouse=True)
def _platform_registered():
    """Self-extend the ``Platform`` enum with ``commy`` so the adapter
    constructs (mirrors ``test_adapter_connection.py``)."""
    manager = PluginManager()
    manifest = PluginManifest(name="commy-platform", kind="platform")
    register(PluginContext(manifest, manager))


def _server_dir() -> str:
    override = os.environ.get("COMMY_SERVER_DIR")
    if override:
        return override
    # tests/ -> clients/hermes -> clients -> repo root
    return str(Path(__file__).resolve().parents[3])


class _RecordingAdapter(CommyAdapter):
    """Captures every routed ``MessageEvent`` so the test can assert on what a
    pod's receive path actually delivered."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handled: list[MessageEvent] = []

    async def handle_message(self, event: MessageEvent) -> None:
        self.handled.append(event)


def _spawn_config(env: dict[str, str], errlog_path: str) -> SpawnConfig:
    return SpawnConfig(
        repo_dir=_server_dir(),
        zulip_site=env["site"],
        minter_email=env["minter_email"],
        minter_api_key=env["minter_api_key"],
        # Disable boot-time channel catch-up: a fresh subscribe must not replay
        # history into the window where the test asserts absence.
        catchup_window_seconds=0,
        # Longer than the whole scenario, so the idle reaper never interferes.
        idle_timeout_seconds=600.0,
        reap_interval_seconds=600.0,
    )


async def _connected_pod(
    env: dict[str, str],
    topic: str,
    errlog_path: str,
) -> _RecordingAdapter:
    """A real pod: real ``TopicConnectionManager`` + real MCP transport, wired
    to the adapter's real receive path, owning one per-topic connection."""
    adapter = _RecordingAdapter(PlatformConfig())
    errlog = open(errlog_path, "a")  # noqa: SIM115 — lives for the pod's lifetime
    manager = make_manager(
        _spawn_config(env, errlog_path),
        adapter.receive_channel_notification,
        errlog=errlog,
    )
    adapter._connection_manager = manager
    adapter._reap_interval_seconds = 600.0
    await adapter.connect()
    await adapter.ensure_topic_connection(env["channel"], topic)
    return adapter


class _ZulipPoster:
    """Stimulus only: posts thread-B / thread-C traffic straight to the realm
    over the Zulip REST API as the minter. Deliberately not a commy
    server — a fourth per-topic server would add a fourth concurrent ``/events``
    long-poll on the one minter identity and trip the per-user rate limit. The
    consumer derives mentions from message *content* (``@**name**``), so a raw
    post pings the same way the ``post`` tool would."""

    def __init__(self, env: dict[str, str]) -> None:
        self._channel = env["channel"]
        self._client = httpx.AsyncClient(
            base_url=env["site"].rstrip("/") + "/api/v1",
            auth=(env["minter_email"], env["minter_api_key"]),
            timeout=30.0,
        )

    async def post(self, topic: str, body: str) -> None:
        # Tolerate a transient per-user 429. The minter is both the admin
        # identity every per-topic pod mints / registers / catches-up through
        # and this stimulus sender, so their REST calls share one Zulip
        # api_by_user budget (a GCRA leaky bucket). When it is briefly full,
        # honor the server's Retry-After in full and retry — Zulip hands back
        # the real drain time, so waiting exactly that long rides out the limit
        # instead of failing a recoverable run. (The pods' /events long-poll is
        # served by Tornado and is not rate-limited; only the boot-burst REST
        # calls and these posts are.)
        for _ in range(POST_RETRY_ATTEMPTS):
            response = await self._client.post(
                "/messages",
                data={"type": "channel", "to": self._channel, "topic": topic, "content": body},
            )
            if response.status_code == 429:
                hint = response.headers.get("retry-after")
                await asyncio.sleep(float(hint) if hint else POST_RETRY_BACKOFF_SECONDS)
                continue
            response.raise_for_status()
            payload = response.json()
            assert payload.get("result") == "success", f"zulip post failed: {payload}"
            return
        raise AssertionError(f"zulip post still rate-limited after retries: {topic}")

    async def aclose(self) -> None:
        await self._client.aclose()


async def _poll_until(predicate, timeout: float, interval: float = 0.25) -> bool:
    waited = 0.0
    while waited < timeout:
        if predicate():
            return True
        await asyncio.sleep(interval)
        waited += interval
    return predicate()


def _texts(adapter: _RecordingAdapter) -> list[str]:
    return [event.text for event in adapter.handled]


def _has(adapter: _RecordingAdapter, marker: str) -> bool:
    return any(marker in text for text in _texts(adapter))


def test_cross_thread_isolation_is_structural_not_conventional():
    env = _ENV
    assert env is not None  # narrowed by the module skipif

    run = uuid.uuid4().hex[:8]
    marker_plain_b = f"a7j6-plain-b-{run}"
    marker_mention_b = f"a7j6-mention-b-{run}"
    marker_plain_b2 = f"a7j6-plain-b2-{run}"
    marker_plain_c = f"a7j6-fence-c-{run}"
    marker_warmup_a = f"a7j6-warmup-a-{run}"
    marker_warmup_b = f"a7j6-warmup-b-{run}"
    marker_warmup_c = f"a7j6-warmup-c-{run}"
    owner_a_name = deterministic_bot_name(env["channel"], TOPIC_A)

    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = os.environ.get("COMMY_TEST_LOG_DIR") or tmp
            errlog = os.path.join(log_dir, "server.stderr.log")

            async with contextlib.AsyncExitStack() as stack:
                # --- poster: raw Zulip REST client, stimulus only (not the SUT) -
                poster = _ZulipPoster(env)
                stack.push_async_callback(poster.aclose)

                async def post(topic: str, body: str) -> None:
                    await poster.post(topic, body)
                    await asyncio.sleep(POD_PACE_SECONDS)

                async def bring_queue_live(
                    pod: _RecordingAdapter, topic: str, marker: str
                ) -> None:
                    """Block until ``pod``'s queue provably captures live traffic
                    on ``topic`` — re-posting the warm-up until it is observed, so
                    a queue that registers slowly can't drop the real traffic that
                    follows."""
                    for _ in range(WARMUP_ATTEMPTS):
                        await post(topic, marker)
                        if await _poll_until(lambda: _has(pod, marker), WARMUP_GAP_SECONDS):
                            return
                    raise AssertionError(f"pod queue for {topic} never went live")

                # --- pods: owner owns thread-A, third owns thread-C ----------
                owner = await _connected_pod(env, TOPIC_A, errlog)
                stack.push_async_callback(owner.disconnect)
                await asyncio.sleep(POD_PACE_SECONDS)

                third = await _connected_pod(env, TOPIC_C, errlog)
                stack.push_async_callback(third.disconnect)

                # Readiness handshake: prove each pod's queue is live on its own
                # topic before any cross-thread traffic, so an absence assertion
                # can never be satisfied merely by a not-yet-registered queue.
                await bring_queue_live(owner, TOPIC_A, marker_warmup_a)
                await bring_queue_live(third, TOPIC_C, marker_warmup_c)

                # === AC1: mentioned in thread-B, isolated from the rest ======
                # Plain first, mention second: the mention is the fence for the
                # plain message's absence.
                await post(TOPIC_B, marker_plain_b)
                await post(TOPIC_B, f"@**{owner_a_name}** {marker_mention_b}")

                got_mention = await _poll_until(
                    lambda: _has(owner, marker_mention_b), ARRIVAL_TIMEOUT_SECONDS
                )
                assert got_mention, "owner never received the cross-thread mention frame"

                await asyncio.sleep(ABSENCE_GRACE_SECONDS)
                assert not _has(owner, marker_plain_b), (
                    "owner received non-mention thread-B traffic before subscribing — "
                    "isolation is conventional, not structural"
                )
                mention_events = [e for e in owner.handled if marker_mention_b in e.text]
                assert mention_events, "mention frame vanished between poll and assert"
                assert all(e.source.thread_id == TOPIC_B for e in mention_events), (
                    "mention frame did not carry thread-B routing"
                )

                # === AC2a: owner chooses to subscribe thread-B ===============
                await owner.ensure_topic_connection(env["channel"], TOPIC_B)
                await bring_queue_live(owner, TOPIC_B, marker_warmup_b)

                await post(TOPIC_B, marker_plain_b2)
                got_b2 = await _poll_until(
                    lambda: _has(owner, marker_plain_b2), ARRIVAL_TIMEOUT_SECONDS
                )
                assert got_b2, "owner did not receive thread-B frames after subscribing"
                b2_events = [e for e in owner.handled if marker_plain_b2 in e.text]
                assert all(e.source.thread_id == TOPIC_B for e in b2_events), (
                    "post-subscribe thread-B frame did not carry thread-B routing"
                )

                # === AC2b: the never-mentioned third pod stays isolated ======
                # plain-C is third's fence: posted after all thread-B traffic,
                # so once third sees it, any B traffic would already have landed.
                await post(TOPIC_C, marker_plain_c)
                got_c = await _poll_until(
                    lambda: _has(third, marker_plain_c), ARRIVAL_TIMEOUT_SECONDS
                )
                assert got_c, "third pod never received its own thread-C fence message"

                for leaked in (marker_plain_b, marker_mention_b, marker_plain_b2):
                    assert not _has(third, leaked), (
                        f"third pod received thread-B traffic ({leaked}) — isolation breached"
                    )
                assert all(e.source.thread_id != TOPIC_B for e in third.handled), (
                    "third pod routed a thread-B frame despite never subscribing or being mentioned"
                )

    asyncio.run(scenario())
