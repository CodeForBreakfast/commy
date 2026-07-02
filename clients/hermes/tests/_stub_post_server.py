"""A minimal real MCP server exposing a ``post`` tool, for the transport's
outbound-delivery test.

Stands in for the commy server so the transport test exercises a real subprocess
+ real MCP ``tools/call`` round-trip with no Zulip realm. It is faithful to the
one behaviour under test: a ``post(channel_name, body, thread)`` tool that
records the arguments it received to ``STUB_POST_RECORD`` (so the test can assert
the transport sent the right channel/topic/body) and returns a result carrying a
``message_id`` (so the test can assert the transport surfaces it).
"""

import json
import os

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("stub-post-commy")


@mcp.tool()
def post(channel_name: str, body: str, thread: str = "") -> dict:
    record = os.environ.get("STUB_POST_RECORD")
    if record:
        with open(record, "w") as handle:
            json.dump(
                {"channel_name": channel_name, "thread": thread, "body": body}, handle
            )
    return {
        "message_id": "stub-msg-1",
        "channel_name": channel_name,
        "thread": thread,
    }


if __name__ == "__main__":
    mcp.run()
