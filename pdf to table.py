import re
import tabula
import pdfplumber
import pandas as pd

PDF_PATH = "input.pdf"
START_PAGE = 957
END_PAGE   = 1180

# Captures ONLY Abc.bac.dbc from:
#   Abc.bac.dbc
#   Abc.bac.dbc table
#   "Abc.bac.dbc table"
#   'Abc.bac.dbc table'
DB_RE = re.compile(
    r'''(?ix)
    ["']?                              # optional starting quote
    \b([A-Za-z_][\w$]*\.[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)\b  # capture db.sch.tbl
    ["']?                              # optional ending quote
    '''
)

def tabula_json_to_df(tjson: dict) -> pd.DataFrame:
    data = tjson.get("data") or []
    rows = [[cell.get("text", "") if isinstance(cell, dict) else "" for cell in row] for row in data]
    df = pd.DataFrame(rows)

    # Trim whitespace, normalize empties
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").reset_index(drop=True)

    return df

def extract_lines_with_y(plumber_page, y_tol=3):
    """
    Group words into lines and return list of {"top": y, "text": line_text}.
    """
    words = plumber_page.extract_words()
    if not words:
        return []

    words = sorted(words, key=lambda w: (w["top"], w["x0"]))

    lines = []
    current = []
    current_y = None

    for w in words:
        y = float(w["top"])
        if current_y is None or abs(y - current_y) <= y_tol:
            current.append(w)
            current_y = y if current_y is None else (current_y + y) / 2
        else:
            lines.append({"top": current_y, "text": " ".join(x["text"] for x in current)})
            current = [w]
            current_y = y

    if current:
        lines.append({"top": current_y, "text": " ".join(x["text"] for x in current)})

    return lines

def find_table_name_above(lines, table_top, max_scan=500):
    """
    Find nearest db.sch.tbl ABOVE table_top (closest y < table_top within max_scan).
    Returns only 'Abc.bac.dbc' even if the line is '"Abc.bac.dbc table"'.
    """
    if table_top is None:
        return ""

    best_y = None
    best_label = ""

    for ln in lines:
        y = float(ln["top"])
        if y < table_top and (table_top - y) <= max_scan:
            m = DB_RE.search(ln["text"])
            if m:
                if best_y is None or y > best_y:
                    best_y = y
                    best_label = m.group(1)

    return best_label

def drop_repeated_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows inside df that exactly match the header values (common on multi-page tables).
    Only applies if df already has named columns.
    """
    header_values = [str(c).strip().lower() for c in df.columns]

    def is_header_row(row):
        row_vals = [str(x).strip().lower() for x in row.tolist()]
        return row_vals == header_values

    return df[~df.apply(is_header_row, axis=1)].reset_index(drop=True)

all_parts = []

with pdfplumber.open(PDF_PATH) as pdf:
    for page_num in range(START_PAGE, END_PAGE + 1):
        # Read tables on this page
        tables_json = tabula.read_pdf(
            PDF_PATH,
            pages=str(page_num),
            multiple_tables=True,
            guess=True,
            silent=True,
            output_format="json"
        )

        if not tables_json:
            continue

        # Extract lines once per page (fast)
        page = pdf.pages[page_num - 1]
        lines = extract_lines_with_y(page, y_tol=3)

        for t in tables_json:
            # 'top' may be missing in some outputs
            top_val = t.get("top", None)
            try:
                table_top = float(top_val) if top_val is not None else None
            except Exception:
                table_top = None

            table_name = find_table_name_above(lines, table_top, max_scan=500) or ""

            df = tabula_json_to_df(t)
            if df.empty:
                continue

            # If extra columns appear, keep first 3 (your table schema)
            if df.shape[1] > 3:
                df = df.iloc[:, :3]

            # If less than 3 columns, skip (or keep if you want)
            if df.shape[1] != 3:
                continue

            df.columns = ["column_name", "description", "comments"]

            # Drop repeated header rows (optional but usually needed)
            df = drop_repeated_header_rows(df)

            # Add columns
            df.insert(0, "table_name", table_name)
            df.insert(1, "__page__", page_num)

            all_parts.append(df)

combined = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()
display(combined)

# Save if needed
# combined.to_csv("tables_957_1180_with_table_name.csv", index=False)
