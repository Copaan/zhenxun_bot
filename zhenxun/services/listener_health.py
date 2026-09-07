"""Local health probes authenticate TLS with the launcher's certificate pin."""

from contextlib import contextmanager
import hashlib
import hmac
import http.client
import ssl
from urllib.parse import urlsplit


@contextmanager
def health_response(url: str, *, fingerprint: str | None = None, timeout: float = 1):
    target = urlsplit(url)
    if target.scheme == "https":
        if not fingerprint:
            raise OSError("health_certificate_pin_missing")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        # Local names need not occur in the certificate; authenticate its DER pin
        # before sending any HTTP data instead of using system CA/name validation.
        context.verify_mode = ssl.CERT_NONE
        connection = http.client.HTTPSConnection(
            target.hostname, target.port, timeout=timeout, context=context
        )
    elif target.scheme == "http":
        connection = http.client.HTTPConnection(
            target.hostname, target.port, timeout=timeout
        )
    else:
        raise ValueError("health_scheme_invalid")
    try:
        connection.connect()
        if target.scheme == "https":
            certificate = connection.sock.getpeercert(binary_form=True)
            actual = hashlib.sha256(certificate).hexdigest()
            if not hmac.compare_digest(actual, fingerprint.lower()):
                raise OSError("health_certificate_pin_mismatch")
        connection.request("GET", target.path or "/")
        with connection.getresponse() as response:
            yield response
    except http.client.HTTPException as error:
        raise OSError("health_response_invalid") from error
    finally:
        connection.close()
