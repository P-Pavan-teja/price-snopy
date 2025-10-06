import hashlib
import string

def format_preserving_encrypt(value: str, key: str = "mysecretkey123") -> str:
    """
    Deterministic, format-preserving encryption.
    - Same input → same output
    - Letters stay letters
    - Numbers stay numbers
    """
    # Create deterministic hash seed
    seed = int(hashlib.sha256((key + value).encode()).hexdigest(), 16)
    
    letters = string.ascii_lowercase
    digits = string.digits
    result = ""

    for ch in value:
        if ch.isalpha():
            base = letters
            idx = (base.index(ch.lower()) + seed) % 26
            new_ch = base[idx]
            result += new_ch.upper() if ch.isupper() else new_ch
        elif ch.isdigit():
            base = digits
            idx = (base.index(ch) + seed) % 10
            result += base[idx]
        else:
            result += ch  # keep special chars like "-" same

    return result

# Examples
print(format_preserving_encrypt("pavan"))        # letters only
print(format_preserving_encrypt("123-45-6789"))  # SSN format
