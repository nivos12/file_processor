"""
Security utilities.

Path traversal prevention: storage paths are always UUID-based; original
filenames are validated here to strip dangerous characters before being
stored as metadata only.

SSRF guard: webhook URLs are resolved and checked against private/loopback
IP ranges before any outbound HTTP call is made.
"""
import ipaddress
import re
import socket
from urllib.parse import urlparse


# ── Filename sanitization ──────────────────────────────────────────────────

_SAFE_FILENAME_RE = re.compile(r"[^\w\s.\-]")


def sanitize_filename(filename: str) -> str:
    """
    Strip path separators and non-printable characters.
    Returns the basename only — caller must never use this as a storage path.
    """
    # Take the basename to prevent directory traversal via the name itself
    name = filename.replace("\\", "/").split("/")[-1]
    # Strip null bytes and control characters
    name = "".join(c for c in name if c.isprintable() and c != "\x00")
    # Replace anything outside safe set with underscore
    name = _SAFE_FILENAME_RE.sub("_", name)
    return name[:255] or "unnamed"


# ── SSRF guard ─────────────────────────────────────────────────────────────

_PRIVATE_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),  # link-local
    ipaddress.ip_network("::1/128"),          # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),         # IPv6 ULA
]


def _is_private_ip(ip_str: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return True  # fail-safe: treat unparseable as private
    return any(addr in net for net in _PRIVATE_NETWORKS)


def validate_webhook_url(url: str) -> None:
    """
    Raise ValueError if the webhook URL resolves to an internal/private address.

    Performs DNS resolution at validation time. This is best-effort — a
    determined attacker with DNS rebinding capability could bypass it.
    For full protection, outbound calls should go through a dedicated egress
    proxy that enforces the same restriction at the network layer.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Webhook URL scheme must be http or https, got {parsed.scheme!r}")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Webhook URL has no hostname")

    # Resolve all A/AAAA records and check each
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as exc:
        raise ValueError(f"Webhook hostname could not be resolved: {exc}") from exc

    for info in infos:
        ip = info[4][0]
        if _is_private_ip(ip):
            raise ValueError(
                f"Webhook URL resolves to a private/internal address ({ip}) — blocked for SSRF prevention"
            )
