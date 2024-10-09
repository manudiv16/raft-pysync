import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

try:

    HAS_CRYPTO = True
except:
    HAS_CRYPTO = False

SALT = b"\x15%q\xe6\xbb\x02\xa6\xf8\x13q\x90\xcf6+\x1e\xeb"


def get_encryptor(password):
    """
    Returns an encryptor object that can be used to encrypt and decrypt data.

    Args:
        password (str): The password used to derive the encryption key.

    Returns:
        cryptography.fernet.Fernet: The encryptor object.

    Raises:
        TypeError: If the password is not a string.
    """
    if not isinstance(password, str):
        raise TypeError("Password must be a string.")

    # Convert the password to bytes
    password = password.encode()

    # Derive the encryption key using PBKDF2HMAC
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=SALT,
        iterations=100000,
        backend=default_backend(),
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))

    # Create and return the Fernet encryptor object
    return Fernet(key)
