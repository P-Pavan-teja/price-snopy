import random, string
from datetime import date, timedelta
from faker import Faker
import pandas as pd

def _gen(pattern, rnd):
    m = {'9': '0123456789', 'A': string.ascii_uppercase, 'a': string.ascii_lowercase, '*': string.ascii_letters + string.digits}
    return ''.join(rnd.choice(m[ch]) if ch in m else ch for ch in pattern)

def _dob(rnd, min_age=18, max_age=80):
    today = date.today()
    start, end = today - timedelta(days=365*max_age), today - timedelta(days=365*min_age)
    return (start + timedelta(days=rnd.randint(0, (end - start).days))).strftime("%m/%d/%Y")

COLUMN_SPECS = [
    {"name": "name",     "fn": lambda f, r, i, row: f.name()},
    {"name": "address",  "fn": lambda f, r, i, row: f.address().replace("\n", ", ")},
    {"name": "email",    "fn": lambda f, r, i, row: f.unique.email()},
    {"name": "customer_id", "fn": lambda f, r, i, row: f"CUST{i:06d}"},
    {"name": "ssn",     "pattern": "999-99-9999"},
    {"name": "social_security_number", "from": "ssn"},
    {"name": "credit_card",        "pattern": "9999-9999-9999-9999"},
    {"name": "credit_card_number", "from": "credit_card"},
    {"name": "phone",        "pattern": "(999) 999-9999"},
    {"name": "phone_number", "from": "phone"},
    {"name": "date_of_birth", "fn": lambda f, r, i, row: _dob(r)},
    {"name": "dob",           "from": "date_of_birth"},
    {"name": "account_number", "fn": lambda f, r, i, row: ''.join(r.choice('0123456789') for _ in range(10))},
    {"name": "routing_number", "fn": lambda f, r, i, row: ''.join(r.choice('0123456789') for _ in range(9))},
    {"name": "passport",       "fn": lambda f, r, i, row: ''.join(r.choice(string.ascii_uppercase + string.digits) for _ in range(9))},
    {"name": "drivers_license","fn": lambda f, r, i, row: ''.join(r.choice(string.ascii_uppercase + string.digits) for _ in range(8))},
]

def generate_fake_table(n_rows=100, seed=42, columns=COLUMN_SPECS):
    rnd = random.Random(seed); fake = Faker(); Faker.seed(seed)
    rows = []
    for i in range(1, n_rows + 1):
        row = {}
        for spec in columns:
            if "from" in spec:        row[spec["name"]] = row[spec["from"]]
            elif "pattern" in spec:   row[spec["name"]] = _gen(spec["pattern"], rnd)
            else:                     row[spec["name"]] = spec["fn"](fake, rnd, i, row)
        rows.append(row)
    return pd.DataFrame(rows)

# quick demo
if __name__ == "__main__":
    df = generate_fake_table(n_rows=1000, seed=123)
    print(df.shape); print(df.head(2))
