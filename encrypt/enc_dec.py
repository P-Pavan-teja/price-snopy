CREATE OR REPLACE FUNCTION FPE_ENC_DIGITS(digits STRING, key STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('cryptography')
HANDLER = 'encrypt'
AS
$$
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import hashlib

def derive_aes_key(key_str: str) -> bytes:
    # Always produce a valid 16-byte AES key from any string
    return hashlib.sha256(key_str.encode("utf-8")).digest()[:16]  # 128-bit key

def aes_prf(key_bytes, data_bytes):
    cipher = Cipher(algorithms.AES(key_bytes), modes.ECB())
    encryptor = cipher.encryptor()
    return encryptor.update(data_bytes) + encryptor.finalize()

def encrypt(digits: str, key: str) -> str:
    if digits is None:
        return None

    digits = digits.strip()
    if not digits.isdigit():
        raise ValueError("Input must be digits only.")

    key_bytes = derive_aes_key(key)
    n = len(digits)

    # Split into two halves for Feistel
    mid = n // 2
    L = digits[:mid]
    R = digits[mid:]

    # If left side is empty (e.g. 1-digit input), just return as-is
    if len(L) == 0:
        return digits

    for round_num in range(6):    # 6 Feistel rounds
        prf_input = (R + str(round_num)).encode("utf-8")
        # pad/truncate to 16 bytes for AES block
        prf_input = prf_input.ljust(16, b'0')[:16]

        prf_output = aes_prf(key_bytes, prf_input)
        # Map PRF output into digit-space of size 10^len(L)
        prf_num = int.from_bytes(prf_output, "big") % (10 ** len(L))

        new_L = R
        new_R = str((int(L) + prf_num) % (10 ** len(L))).zfill(len(L))
        L, R = new_L, new_R

    return L + R
$$;
CREATE OR REPLACE FUNCTION FPE_DEC_DIGITS(enc STRING, key STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('cryptography')
HANDLER = 'decrypt'
AS
$$
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import hashlib

def derive_aes_key(key_str: str) -> bytes:
    # Same derivation as encrypt – must match exactly
    return hashlib.sha256(key_str.encode("utf-8")).digest()[:16]

def aes_prf(key_bytes, data_bytes):
    cipher = Cipher(algorithms.AES(key_bytes), modes.ECB())
    encryptor = cipher.encryptor()
    return encryptor.update(data_bytes) + encryptor.finalize()

def decrypt(enc: str, key: str) -> str:
    if enc is None:
        return None

    enc = enc.strip()
    if not enc.isdigit():
        raise ValueError("Input must be digits only.")

    key_bytes = derive_aes_key(key)
    n = len(enc)

    mid = n // 2
    L = enc[:mid]
    R = enc[mid:]

    if len(L) == 0:
        return enc

    # Reverse Feistel rounds
    for round_num in reversed(range(6)):
        prf_input = (L + str(round_num)).encode("utf-8")
        prf_input = prf_input.ljust(16, b'0')[:16]

        prf_output = aes_prf(key_bytes, prf_input)
        prf_num = int.from_bytes(prf_output, "big") % (10 ** len(L))

        new_R = L
        new_L = str((int(R) - prf_num) % (10 ** len(L))).zfill(len(L))
        L, R = new_L, new_R

    return L + R
$$;

-- Encrypt
SELECT FPE_ENC_DIGITS('1234564567898765435678909876545678987654', 'MYSECRETKEY') AS enc;

SELECT FPE_DEC_DIGITS('5348364276859263653044996159374674273738', 'MYSECRETKEY') AS dec;
