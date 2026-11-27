"""Reachability only: no credentials, redirects, cluster operations or endpoint names in logs."""
import concurrent.futures
import http.client
import ipaddress
import json
import os
import socket
import ssl


def addresses(host, port):
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return {ipaddress.ip_address(i[4][0]) for i in infos}


def probe(host, port):
    if not host or any(c in host for c in "/@:\r\n"):
        raise ValueError("invalid configured hostname")
    try:
        ips = addresses(host, port)
        if not ips or not all(ip.is_global for ip in ips):
            return "Private/non-public DNS; no private route configured"
        if port == 22:
            with socket.create_connection((host, 22), timeout=8) as sock:
                return "SSH banner received (no login)" if sock.recv(128).startswith(b"SSH-") else "TCP reachable; SSH unconfirmed"
        conn = http.client.HTTPSConnection(host, timeout=10, context=ssl.create_default_context())
        try:
            conn.request("GET", "/api/2.0/clusters/spark-versions", headers={"Accept": "application/json"})
            result = conn.getresponse()
            body = result.read(65536).lower()
            if result.status in (401, 403) and any(x in body for x in (
                    b"ip access list", b"ip_access_list", b"network access", b"ip acl", b"source ip", b"ip address is blocked",
                    b"private link", b"privatelink", b"public access is not allowed")):
                return f"HTTPS {result.status}: network policy denied"
            if result.status == 401:
                return "HTTPS 401: connected; authentication required"
            if result.status == 200 and b'"versions"' in body:
                return "HTTPS 200: runtime metadata reachable"
            return f"HTTPS {result.status}: connected; API access unconfirmed"
        finally:
            conn.close()
    except socket.gaierror:
        return "DNS lookup failed"
    except (TimeoutError, socket.timeout):
        return "Timed out"
    except ssl.SSLError:
        return "TLS verification/handshake failed"
    except (OSError, http.client.HTTPException) as error:
        return f"Connection failed ({type(error).__name__})"


profiles = json.loads(os.environ["DB_NETWORK_TARGETS"])
checks = [(p, "workspace HTTPS", row["host"], 443) for p, row in profiles.items()]
checks += [(p, "bastion SSH", row["jump"], 22) for p, row in profiles.items() if "jump" in row]
with concurrent.futures.ThreadPoolExecutor(max_workers=len(checks)) as executor:
    results = list(executor.map(lambda item: probe(item[2], item[3]), checks))
lines = ["| Profile | Path | GitHub-hosted runner result |", "| --- | --- | --- |"]
lines += [f"| {p} | {target} | {result} |" for (p, target, _, _), result in zip(checks, results)]
print("\n".join(lines))
with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
    print("\n".join(lines), file=summary)
