#!/usr/bin/env python3
"""The brain, shared edition: the core (claude_usage_exporter.py) and the shared
drivers. Run: python3 brain.py. Each driver registers itself on import."""
import sys

import claude_usage_exporter as core
# Registration order is the order every payload lists providers in: brain_household.py's, less its own.
from providers import anthropic_subscription, kimi, codex, local, anthropic_api  # noqa: F401 — each registers itself

if __name__ == "__main__":
    try:
        core.main()
    except KeyboardInterrupt:
        sys.exit(0)
