"""Generates a server.crt / server.key to use on various backends for TLS/SSL modes."""

import datetime

# pip install cryptography
from cryptography import x509
from cryptography.hazmat.backends import default_backend as dfb
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

EXPIRE_DAYS = 36500
PUBLIC_EXPONENT = 65537
KEY_SIZE = 4096


def load_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    """Load the CA key and certificate pems for signing."""
    with open("ca.key", "rb") as fh:
        key = serialization.load_pem_private_key(data=fh.read(), password=None, backend=dfb())
    with open("ca.crt", "rb") as fh:
        crt = x509.load_pem_x509_certificate(data=fh.read(), backend=dfb())
    return key, crt


def mk_server_pair():
    """Create server.key and server.crt signed by ca.key."""
    ca_key, ca_crt = load_ca()
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    exp = now + datetime.timedelta(days=EXPIRE_DAYS)
    srl = x509.random_serial_number()
    key = rsa.generate_private_key(public_exponent=PUBLIC_EXPONENT, key_size=KEY_SIZE, backend=dfb())
    sub = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "127.0.0.1")])
    crt = x509.CertificateBuilder().subject_name(sub)
    crt = crt.issuer_name(ca_crt.issuer)
    crt = crt.public_key(key.public_key())
    crt = crt.serial_number(srl)
    crt = crt.not_valid_after(exp)
    crt = crt.not_valid_before(now)
    crt = crt.sign(private_key=ca_key, algorithm=hashes.SHA256(), backend=dfb())
    with open("server.key", "wb") as fh:
        fh.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    with open("server.crt", "wb") as fh:
        fh.write(crt.public_bytes(encoding=serialization.Encoding.PEM))


if __name__ == "__main__":
    mk_server_pair()
