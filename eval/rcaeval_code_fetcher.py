"""
rcaeval_code_fetcher.py - Fetch source code snippets from GitHub for RE3-OB cases.

When stack traces in logs reference a file and line number, this module fetches
the surrounding source code from the GoogleCloudPlatform/microservices-demo repo
so the LLM can see *exactly* what the faulty code looks like — not just the error.

Example:  "NullPointerException at CartService.java:142"
          → fetch CartService.java lines 127–157 from GitHub
          → LLM sees:   cart = cartStore.getCart(userId);  // line 142 can be null
                         cart.addItem(productId, quantity);  // NPE here

Results are cached to disk (.cache/github_code/) so each file is fetched at most
once per project, surviving multiple eval runs and avoiding GitHub rate limits.
Unauthenticated GitHub raw access allows 60 req/hr; set GITHUB_TOKEN env var for
5000 req/hr.

Supported languages and stack trace formats:
  Java    — at hipstershop.CartService.addItem(CartService.java:142)
  Python  — File "/app/recommendation_server.py", line 42
  Go      — /app/main.go:87  or  checkout.go:87
  C#      — in /app/src/CartStore.cs:line 42
  Node.js — at /app/server.js:42:15
"""

import hashlib
import os
import re
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()


# ---------------------------------------------------------------------------
# GitHub configuration
# ---------------------------------------------------------------------------

_REPO_OWNER = "GoogleCloudPlatform"
_REPO_NAME  = "microservices-demo"
_REPO_REF   = "main"    # branch / commit to fetch from
_RAW_BASE   = f"https://raw.githubusercontent.com/{_REPO_OWNER}/{_REPO_NAME}/{_REPO_REF}"

# Disk cache directory (project root / .cache / github_code)
_CACHE_DIR  = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", ".cache", "github_code")
)

# In-memory cache: URL → list of file lines (None = 404 / fetch failed)
_FILE_CACHE: Dict[str, Optional[List[str]]] = {}


# ---------------------------------------------------------------------------
# Service metadata
# ---------------------------------------------------------------------------

# Each service's source directory within the repo
_SERVICE_SOURCE_DIRS: Dict[str, str] = {
    "adservice":             "src/adservice/src/main/java/hipstershop",
    "cartservice":           "src/cartservice/src",
    "checkoutservice":       "src/checkoutservice",
    "currencyservice":       "src/currencyservice",
    "emailservice":          "src/emailservice",
    "frontend":              "src/frontend",
    "paymentservice":        "src/paymentservice",
    "productcatalogservice": "src/productcatalogservice",
    "recommendationservice": "src/recommendationservice",
    "shippingservice":       "src/shippingservice",
    "loadgenerator":         "src/loadgenerator",
}

# Service → primary language (determines which stack trace parser to use)
_SERVICE_LANGUAGE: Dict[str, str] = {
    "adservice":             "java",
    "cartservice":           "csharp",
    "checkoutservice":       "go",
    "currencyservice":       "nodejs",
    "emailservice":          "python",
    "frontend":              "go",
    "paymentservice":        "nodejs",
    "productcatalogservice": "go",
    "recommendationservice": "python",
    "shippingservice":       "go",
    "loadgenerator":         "python",
}


# ---------------------------------------------------------------------------
# Stack trace parsers — extract (filename, line_number) pairs
# ---------------------------------------------------------------------------

# Java:  at hipstershop.CartService.addItem(CartService.java:142)
#        (tab is optional — may be stripped when extracted from CSV logs)
_JAVA_RE = re.compile(
    r"\t?at [\w$.]+\.[\w<>$]+\(([\w]+\.java):(\d+)\)"
)

# Python:  File "/app/recommendation_server.py", line 42
_PYTHON_RE = re.compile(
    r'File ".*?/?([\w.]+\.py)", line (\d+)'
)

# Go:  /app/main.go:87  or  checkout.go:87 +0x...
_GO_RE = re.compile(
    r'(?:^|[\s(])(?:.*/)?(\w[\w.-]*\.go):(\d+)'
)

# C#:  in /app/src/CartStore.cs:line 42
_CSHARP_RE = re.compile(
    r' in .*?/?([\w.]+\.cs):line (\d+)'
)

# Node.js:  at Object.<anonymous> (/app/server.js:42:15)  or  at /app/server.js:42:15
_NODEJS_RE = re.compile(
    r'(?:at |[( ])(?:.*/)?(\w[\w.-]*\.js):(\d+):\d+'
)

_LANGUAGE_PARSERS: Dict[str, re.Pattern] = {
    "java":    _JAVA_RE,
    "python":  _PYTHON_RE,
    "go":      _GO_RE,
    "csharp":  _CSHARP_RE,
    "nodejs":  _NODEJS_RE,
}


def parse_code_refs(
    log_message: str,
    service: str,
) -> List[Tuple[str, int]]:
    """
    Extract (filename, line_number) pairs from a stack trace log message.

    Uses the language-specific parser for the given service, then falls back
    to trying all parsers (in case the service runs a polyglot stack).

    Returns a deduplicated list, limited to 5 refs per message to keep
    downstream fetch volume bounded.

    Examples:
        "at CartService.addItem(CartService.java:142)"  → [("CartService.java", 142)]
        'File "/app/server.py", line 55'                → [("server.py", 55)]
    """
    lang    = _SERVICE_LANGUAGE.get(service)
    parsers = []
    if lang and lang in _LANGUAGE_PARSERS:
        parsers.append(_LANGUAGE_PARSERS[lang])
    # Also try all parsers — services sometimes log errors from vendored libs
    for l, p in _LANGUAGE_PARSERS.items():
        if l != lang:
            parsers.append(p)

    seen: set = set()
    refs: List[Tuple[str, int]] = []
    for parser in parsers:
        for m in parser.finditer(log_message):
            filename = m.group(1)
            line_num = int(m.group(2))
            key = (filename, line_num)
            if key not in seen:
                seen.add(key)
                refs.append(key)
            if len(refs) >= 5:
                return refs
    return refs


# ---------------------------------------------------------------------------
# GitHub fetching + caching
# ---------------------------------------------------------------------------

def _disk_cache_path(url: str) -> str:
    """Map a URL to a stable disk cache path using its MD5."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(_CACHE_DIR, url_hash + ".txt")


def _fetch_file_lines(
    url: str,
    token: Optional[str] = None,
) -> Optional[List[str]]:
    """
    Fetch file content from a URL and return as a list of lines.
    Uses in-memory cache first, then disk cache, then live fetch.
    Returns None on 404 or network error.
    """
    # 1. In-memory cache
    if url in _FILE_CACHE:
        return _FILE_CACHE[url]

    # 2. Disk cache
    cache_path = _disk_cache_path(url)
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
        _FILE_CACHE[url] = lines
        return lines

    # 3. Live fetch
    headers: Dict[str, str] = {"User-Agent": "RootScout-RCAEval/1.0"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            _FILE_CACHE[url] = None   # negative cache — don't retry
        else:
            print(f"[code_fetcher] HTTP {e.code} fetching {url}")
        return None
    except Exception as e:
        print(f"[code_fetcher] Error fetching {url}: {e}")
        return None

    lines = content.splitlines()

    # Save to disk cache
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        f.write(content)

    _FILE_CACHE[url] = lines
    return lines


def _build_raw_url(service: str, filename: str) -> Optional[str]:
    """
    Construct the raw.githubusercontent.com URL for a given service + filename.
    Returns None if the service has no known source directory.
    """
    src_dir = _SERVICE_SOURCE_DIRS.get(service)
    if src_dir is None:
        return None
    return f"{_RAW_BASE}/{src_dir}/{filename}"


def _format_snippet(
    lines: List[str],
    error_line: int,
    context: int = 15,
) -> str:
    """
    Extract ±context lines around error_line and format with line numbers.
    The error line is marked with '>>>' for immediate visual identification.

    Example output:
        138:     public Cart getCart(String userId) {
        139:         ...
      >>> 142:         return null;   // <-- fault injected here
        143:     }
    """
    start = max(0, error_line - context - 1)
    end   = min(len(lines), error_line + context)
    parts = []
    for i, line in enumerate(lines[start:end], start=start + 1):
        marker = ">>>" if i == error_line else "   "
        parts.append(f"{marker} {i:5d}: {line}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_code_snippet(
    service: str,
    filename: str,
    line_num: int,
    context_lines: int = 15,
    token: Optional[str] = None,
) -> Optional[str]:
    """
    Fetch source code around a specific file + line for an Online Boutique service.

    Args:
        service:       OB service name (e.g. "cartservice")
        filename:      Source file name from stack trace (e.g. "CartService.java")
        line_num:      The line referenced in the stack trace
        context_lines: How many lines above and below to include (default 15)
        token:         GitHub personal access token (increases rate limit)

    Returns:
        Formatted code snippet string, or None if the file cannot be fetched.
    """
    url = _build_raw_url(service, filename)
    if url is None:
        return None

    lines = _fetch_file_lines(url, token=token)
    if lines is None:
        return None

    if line_num < 1 or line_num > len(lines):
        # Line out of range — return full file head instead
        snippet = _format_snippet(lines, line_num=1, context=min(20, len(lines)))
        return snippet

    return _format_snippet(lines, line_num, context=context_lines)


def enrich_with_code_snippets(
    trace_events: List[Dict[str, Any]],
    service: str,
    inject_ts: str,
    max_files: int = 3,
    context_lines: int = 15,
) -> List[Dict[str, Any]]:
    """
    Given the stack-trace log events for a service, fetch the corresponding
    source code snippets from GitHub and return them as new source_code events.

    These events are added to the node alongside the stack trace events so the
    LLM sees both the runtime error AND the actual code that caused it.

    Args:
        trace_events:  The code_fault events already detected for this service
        service:       OB service name
        inject_ts:     Timestamp string for the events (e.g. fault inject time)
        max_files:     Max number of distinct source files to fetch (default 3)
        context_lines: Lines of source context around the fault line

    Returns:
        List of source_code events (may be empty if no refs found or fetch fails)
    """
    token = os.getenv("GITHUB_TOKEN")

    # Collect all (filename, line) refs from trace payloads, deduped
    seen_files: set = set()
    file_refs: List[Tuple[str, int]] = []

    for event in trace_events:
        msg = event.get("payload", {}).get("log_message", "")
        refs = parse_code_refs(msg, service)
        for filename, line_num in refs:
            if filename not in seen_files:
                seen_files.add(filename)
                file_refs.append((filename, line_num))
            if len(file_refs) >= max_files:
                break
        if len(file_refs) >= max_files:
            break

    if not file_refs:
        return []

    code_events: List[Dict[str, Any]] = []
    for filename, line_num in file_refs:
        snippet = fetch_code_snippet(
            service=service,
            filename=filename,
            line_num=line_num,
            context_lines=context_lines,
            token=token,
        )
        if snippet is None:
            continue

        url = _build_raw_url(service, filename) or ""
        code_events.append({
            "source":    "source_code",
            "kind":      "code_snippet",
            "timestamp": inject_ts,
            "summary":   f"{filename}:{line_num} — source context ({_REPO_REF})",
            "payload": {
                "filename":   filename,
                "patch":      snippet,          # reuses agent's existing patch renderer
                "sha":        _REPO_REF,
                "github_url": url,
            },
        })

    if code_events:
        print(
            f"[code_fetcher] Fetched {len(code_events)} code snippet(s) "
            f"for {service}: {[e['payload']['filename'] for e in code_events]}"
        )

    return code_events
