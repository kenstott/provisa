#!/usr/bin/env python3
import json
import sys

d = json.load(sys.stdin)
f = d.get("tool_input", {}).get("file_path", "")
if not f.endswith(".md"):
    sys.exit(0)
if "/.claude/" in f:
    sys.exit(0)
try:
    text = open(f).read().lower()
except Exception:
    sys.exit(0)

banned = [
    "load-bearing",
    "dressed up as",
    "delve",
    "tapestry",
    "nuanced",
    "multifaceted",
    "holistic",
    "leverage",
    "robust",
    "streamline",
    "foster",
    "paradigm",
    "synergy",
    "innovative",
    "comprehensive",
    "navigate",
    "unlock",
    "harness",
    "empower",
    "elevate",
    "realm",
    "facet",
    "myriad",
    "plethora",
    "pivotal",
    "seamless",
    "that being said",
    "with that in mind",
    "moving forward",
    "at the end of the day",
    "it is worth noting",
    "it's worth noting",
    "it should be noted",
    "in conclusion",
    "to summarize",
    "as we navigate",
    "the intersection of",
    "testament to",
    "dive into",
    "unpack",
]

hits = [p for p in banned if p in text]
if hits:
    msg = "AI-smell prose in " + f + ": " + ", ".join(hits)
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PostToolUse",
                    "permissionDecision": "block",
                    "permissionDecisionReason": msg,
                }
            }
        )
    )
    sys.exit(2)
