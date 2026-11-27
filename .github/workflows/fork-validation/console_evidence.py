# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read bounded, public excerpts of the fork pilot's driver logs."""

import json
import os
import re
import stat
from ipaddress import IPv6Address
from urllib.parse import urlsplit


_WINDOW = 256 * 1024
_BUDGET = 56 * 1024
_OMITTED = "[... omitted ...]"
_SGR = re.compile(rb"\x1b\[[0-9;:]*m")
# Only clean, public repository URLs may leave the driver. Never echo URL credentials,
# query strings, fragments or private workspaces simply because a download failed.
_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s<>\"']+", re.I)
_PUBLIC_REPOS = {
    "repo.maven.apache.org", "repo1.maven.org", "repository.apache.org", "archive.apache.org",
    "downloads.apache.org", "maven-central.storage-download.googleapis.com",
    "oss.sonatype.org", "s01.oss.sonatype.org", "central.sonatype.com", "repo.scala-sbt.org",
    "pypi.org", "files.pythonhosted.org", "github.com", "raw.githubusercontent.com",
}
_SECRET = re.compile(
    r"(?:\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*:|\bBearer\s+[\w+/=-]+|"
    r"\bBasic\s+[A-Za-z0-9+/]{12,}={0,2}\b|"
    r"(?<![a-z0-9])(?:password|passwd|pwd|passphrase|(?:client[_-]?)?secret|api[_-]?key|"
    r"(?:secret[_-]?)?access[_-]?key(?:[_-]?id)?|sig(?:nature)?|(?:sharedaccess)?signature|"
    r"(?:storage)?account[_-]?key|fs\.azure\.account\.key(?:\.[\w.-]+)?|credential[s]?|"
    r"x-amz-security-token|(?:access|refresh|session|id)[_-]?token)[\"']?\s*[:=]|"
    r"token\s*=|[\"']token[\"']\s*:|^\s*token\s*:|--(?:password|token|secret|api-key)\s+\S|"
    r"<(?:password|passphrase|privateKey|secret|token)\b|"
    r"\b(?:gh[pousr]_\w{12,}|github_pat_\w+|glpat-[\w-]{10,}|dapi[\da-f]{16,}|(?:AKIA|ASIA)[A-Z0-9]{16}|"
    r"xox[baprs]-[\w-]+|eyJ[\w-]{6,}\.[\w-]+\.[\w-]+|ssh-(?:rsa|ed25519)\s+\S+)|"
    r"^\s*(?:machine\s+\S+\s+|(?:login|password|passwd)\s+\S)|\b(?:Proc-Type|DEK-Info):)", re.I)
_NETWORK = re.compile(
    r"\b(?:[\w-]+\.)+(?:internal|localdomain|corp|local|databricks\.(?:com|net)|"
    r"nvidia\.com|amazonaws\.com|azure\.com|azuredatabricks\.net)\b|"
    r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b|\b(?:[0-9a-f]{2}:){5}[0-9a-f]{2}\b|"
    r"\b(?:ip-\d+-\d+-\d+-\d+|i-[0-9a-f]{8,17}|\d{4}-\d{6}-[a-z0-9]{6,})\b|"
    r"\b(?:hostname|cluster[_. -]?(?:id|name)|workspace[_. -]?(?:id|url)|host|UnknownHostException)\s*[:=]\s*\S|"
    r"[\w.-]+@[\w.-]+", re.I)
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9a-f]{0,4}:){2,}[0-9a-f:.]*(?!\w)", re.I)
_COMMAND = re.compile(
    r"^(?:\s*\[[A-Z]+\]\s*)?\s*(?:::[\w-]+(?:[ :]|$)|##(?:vso)?\[|"
    r"(?:export\s+|declare -x\s+)?[A-Z_][A-Z_0-9]*=)")
_GH_COMMAND = re.compile(r"::(?:warning|notice|error|debug|add-mask|stop-commands|set-output|"
                         r"set-env|save-state|add-path|group|endgroup|echo)(?: [^\r\n:]+)?::", re.I)
_ARMOR = re.compile(rb"-----(BEGIN|END) [A-Z0-9 ]+-----")
# At an unknown byte boundary don't publish unframed text as if it began a new
# record. Once a new build record starts, ordinary lines need no known vocabulary.
_RECORD = re.compile(rb"^(?:\[(?:INFO|WARN(?:ING)?|ERROR|DEBUG|FATAL|JUnit|driver|GPU)\b|"
                     rb"\d{2,4}[-/]\d\d[-/]\d\d[ T]|=== |Traceback \(|(?:FAIL|ERROR): )")
_HEREDOC = re.compile(r"<<-?\s*['\"]?([A-Za-z_]\w*)['\"]?")
_XML = re.compile(r"<(/?)(password|passphrase|privateKey|secret|token)>", re.I)
_TRACE = re.compile(r"^\s*(?:\[[A-Z]+\]\s*)?(?:\++\s|\$\s|>>>\s|(?:Environment|Environment variables):\s*$)")
_ENCODED_RECORD = re.compile(rb"\[[A-Z]+\]\s+[A-Za-z0-9_+/=-]+\s*")
_TRUNCATED = re.compile(rb"\[\.\.\. \d+ bytes omitted \.\.\.\]")


def _safe_text(line):
    if (_NETWORK.search(line) or _GH_COMMAND.search(line)
            or not all(c == "\t" or c.isprintable() for c in line)):
        return False
    for match in _IPV6.finditer(line):
        if match[0] != "::":
            try:
                IPv6Address(match[0])
            except ValueError:
                continue
            return False

    def public_url(match):
        try:
            url = urlsplit(match[0].rstrip(".,);]"))
            return (url.scheme in ("https", "http") and url.hostname in _PUBLIC_REPOS
                    and url.username is None and url.password is None and not url.query
                    and not url.fragment and url.port in (None, 80, 443))
        except ValueError:
            return False

    return all(public_url(m) for m in _URL.finditer(line))


def _public_lines(records, clipped):
    public = []
    armor = False
    until = None
    private_indent = None
    for raw in records:
        # Process delimiters even if their line is otherwise unsafe. If a fragment
        # starts with an orphan END, discard its potentially private leading text.
        markers = _ARMOR.findall(raw)
        if markers:
            for marker in markers:
                if marker == b"END" and not armor:
                    public.clear()
                armor = marker == b"BEGIN"
            clipped = False
            continue
        if armor:
            continue
        if _TRUNCATED.fullmatch(raw):
            public.append(_OMITTED)
            clipped = True
            continue
        try:
            line = raw.removesuffix(b"\r").decode("utf-8")
        except UnicodeDecodeError:
            continue
        if until is not None:
            if until.fullmatch(line.strip()):
                until = None
            continue
        indent = len(line) - len(line.lstrip())
        if private_indent is not None:
            if not line.strip() or indent > private_indent:
                continue
            private_indent = None
        xml = _XML.search(line)
        if xml and xml[1] == "/":
            public.clear()  # Unmatched closing tag in a fragment supplied by the driver.
            clipped = False
            continue
        trace = _TRACE.search(line)
        if trace or _SECRET.search(line) or _COMMAND.search(line):
            if trace or line.rstrip().endswith((":", "=")):
                clipped = True  # An xtraced echo/printenv can print an arbitrary next line.
            heredoc = _HEREDOC.search(line)
            if heredoc:
                until = re.compile(re.escape(heredoc[1]))
            elif xml and "</" + xml[2].lower() + ">" not in line.lower():
                until = re.compile(r".*</" + re.escape(xml[2]) + r">.*", re.I)
            elif line.count("'") % 2 or line.count('"') % 2:
                quote = "'" if line.count("'") % 2 else '"'
                until = re.compile(r".*" + quote + r"[,;)]?")
            elif re.search(r"[:=]\s*[|>][+-]?\s*$", line) or re.match(
                    r"\s*(?:proxy-authorization|authorization|cookie|set-cookie)\s*:", line, re.I):
                private_indent = indent  # Folded HTTP header or YAML block scalar.
            continue
        if clipped and (not _RECORD.match(raw) or _ENCODED_RECORD.fullmatch(raw)):
            continue
        clipped = False
        if len(raw) > 4096:
            public.append(_OMITTED)
        elif _safe_text(line):
            public.append(line if len(line) <= 500 else line[:500] + _OMITTED)
    return public


def _window(stream, start, end, finish_line):
    # Inspect the previous byte to avoid publishing a suffix of a private or oversized line.
    stream.seek(max(0, start - 1))
    raw = stream.read(end - start + int(start > 0))
    if start:
        raw = raw.partition(b"\n")[2]
    if not finish_line:
        complete, sep, _ = raw.rpartition(b"\n")
        if not sep:
            return []
        raw = complete
    else:
        raw = raw.removesuffix(b"\n")
    return _public_lines(_SGR.sub(b"", raw).split(b"\n"), bool(start)) if raw else []


def _take(lines, count, budget, *, tail):
    kept = []
    size = 2  # JSON array brackets; cost below includes the separator.
    for line in reversed(lines) if tail else lines:
        cost = len(json.dumps(line)) + 2
        if len(kept) == count or size + cost > budget:
            return (kept[::-1] if tail else kept), True
        kept.append(line)
        size += cost
    return (kept[::-1] if tail else kept), False


def _excerpt(directory_fd, filename, finished, ok):
    try:
        fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except FileNotFoundError:
        return []
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise ValueError("Expected a regular private log file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            end = st.st_size
            if not finished or ok or filename != "diagnostics.log":
                lines = _window(stream, max(0, end - _WINDOW), end, finished)
                count = 2 if not finished else (1000 if ok else 10)
                budget = _BUDGET - (len(_OMITTED) + 4 if ok and finished else 0)
                tail, clipped = _take(lines, count, budget, tail=True)
                if ok and finished and len(tail) < count and (clipped or end > _WINDOW):
                    tail.insert(0, _OMITTED)
                return tail

            if end <= 2 * _WINDOW:
                first = last = _window(stream, 0, end, True)
                entire, overflow = _take(first, 1000, _BUDGET, tail=False)
                if not overflow:
                    return entire
            else:
                first = _window(stream, 0, _WINDOW, False)
                last = _window(stream, end - _WINDOW, end, True)
            half = (_BUDGET - len(json.dumps(_OMITTED)) - 8) // 2
            return (_take(first, 499, half, tail=False)[0] + [_OMITTED]
                    + _take(last, 500, half, tail=True)[0])
    finally:
        os.close(fd)


def snapshot(directory, finished=False, ok=False):
    """Return stdout, stderr and diagnostics lists; no reads outside these three files."""
    names = ("stdout", "stderr", "diagnostics")
    try:
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return {name: [] for name in names}
    try:
        return {name: _excerpt(fd, name + ".log", finished, ok) for name in names}
    finally:
        os.close(fd)
