import re
import tabula
import pdfplumber
import pandas as pd

PDF_PATH   = "databases/Databases.pdf"
START_PAGE = 1
END_PAGE   = 300   # set your internal pages

# Capture only Abc.bac.dbc from:  Abc.bac.dbc table  /  "Abc.bac.dbc table"
DB_RE = re.compile(r'(?i)\b([A-Za-z_][\w$]*\.[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)\b')

def tabula_json_to_df(tjson: dict) -> pd.DataFrame:
    data = tjson.get("data") or []
    rows = [[cell.get("text", "") if isinstance(cell, dict) else "" for cell in row] for row in data]
    df = pd.DataFrame(rows)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").reset_index(drop=True)
    return df

def normalize_3col(df: pd.DataFrame):
    # enforce exactly 3 cols
    if df.shape[1] > 3:
        df = df.iloc[:, :3]
    if df.shape[1] != 3:
        return None
    df.columns = ["column_name", "description", "comments"]
    return df

def drop_repeated_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    header_vals = [c.strip().lower() for c in df.columns]
    def is_header_row(row):
        vals = [str(x).strip().lower() for x in row.tolist()]
        return vals == header_vals
    return df[~df.apply(is_header_row, axis=1)].reset_index(drop=True)

def extract_lines_with_y(plumber_page, y_tol=3):
    words = plumber_page.extract_words()
    if not words:
        return []
    words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines, cur, cur_y = [], [], None
    for w in words:
        y = float(w["top"])
        if cur_y is None or abs(y - cur_y) <= y_tol:
            cur.append(w)
            cur_y = y if cur_y is None else (cur_y + y) / 2
        else:
            lines.append({"top": cur_y, "text": " ".join(x["text"] for x in cur)})
            cur, cur_y = [w], y
    if cur:
        lines.append({"top": cur_y, "text": " ".join(x["text"] for x in cur)})
    return lines

def find_label_near_table(lines, table_top, max_scan=700):
    """
    Find the closest db.sch.tbl ABOVE THIS TABLE (not just anywhere on page).
    This is the only thing that is allowed to "start a new table_name".
    """
    if table_top is None:
        return None
    best_y = None
    best = None
    for ln in lines:
        y = float(ln["top"])
        if y < table_top and (table_top - y) <= max_scan:
            m = DB_RE.search(ln["text"])
            if m:
                if best_y is None or y > best_y:
                    best_y = y
                    best = m.group(1)
    return best

def safe_float(x):
    try:
        return float(x)
    except:
        return None

# ---------------------------------------------------------
# Main: TABLE-BY-TABLE assignment using active_label
# ---------------------------------------------------------
all_parts = []
active_label = ""   # this is the current table_name (tbl1 while continuing)

with pdfplumber.open(PDF_PATH) as pdf:
    total_pages = len(pdf.pages)
    end = min(END_PAGE, total_pages)

    for page_num in range(START_PAGE, end + 1):
        # Extract tables JSON for this page
        try:
            tables_json = tabula.read_pdf(
                PDF_PATH,
                pages=str(page_num),
                multiple_tables=True,
                guess=True,
                silent=True,
                output_format="json"
            )
        except Exception as e:
            print(f"Tabula failed on page {page_num}: {e}")
            continue

        if not tables_json:
            continue

        # Extract lines once
        lines = extract_lines_with_y(pdf.pages[page_num - 1], y_tol=3)

        # IMPORTANT: process tables TOP->BOTTOM so state machine is correct
        # Some tabula versions may not guarantee order
        tables_json = sorted(tables_json, key=lambda t: safe_float(t.get("top")) or 1e9)

        for t in tables_json:
            table_top = safe_float(t.get("top"))

            # 1) Try to find a label near/above THIS table (new table start)
            label = find_label_near_table(lines, table_top, max_scan=700)

            # If found, this table starts a new context -> update active_label
            if label:
                active_label = label

            # 2) If not found, treat as continuation -> use active_label
            # (This is what fixes your mix-up)
            use_label = active_label

            df = tabula_json_to_df(t)
            if df.empty:
                continue

            df = normalize_3col(df)
            if df is None:
                continue

            df = drop_repeated_header_rows(df)

            df.insert(0, "table_name", use_label)
            df.insert(1, "__page__", page_num)
            all_parts.append(df)

combined = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()
display(combined)

# combined.to_csv("tables_with_table_name.csv", index=False)
