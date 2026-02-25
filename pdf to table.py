import re
import tabula
import pdfplumber
import pandas as pd

PDF_PATH   = "databases/Databases.pdf"
START_PAGE = 957
END_PAGE   = 1180   # change as needed

# ✅ handles db.sch.tbl and db.sch. tbl and db . sch . tbl
DB_RE = re.compile(r'(?i)\b([A-Za-z_][\w$]*)\s*\.\s*([A-Za-z_][\w$]*)\s*\.\s*([A-Za-z_][\w$]*)\b')

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def normalize_db_label(match):
    return f"{match.group(1)}.{match.group(2)}.{match.group(3)}"

def tabula_json_to_df(tjson):
    data = tjson.get("data") or []
    rows = [[cell.get("text", "") if isinstance(cell, dict) else "" for cell in row] for row in data]
    df = pd.DataFrame(rows)

    # strip strings, drop fully empty rows
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").reset_index(drop=True)
    return df

def normalize_3col(df):
    if df.shape[1] > 3:
        df = df.iloc[:, :3]
    if df.shape[1] != 3:
        return None
    df.columns = ["column_name", "description", "comments"]
    return df

# ✅ remove per-table header row (source-level)
def remove_table_header_rows(df):
    col = df["column_name"].fillna("").astype(str).str.strip().str.lower()
    des = df["description"].fillna("").astype(str).str.strip().str.lower()
    com = df["comments"].fillna("").astype(str).str.strip().str.lower()

    is_header = (
        col.str.match(r"^column(\s*name)?$", na=False) &
        des.eq("description") &
        com.str.match(r"^comments(\s*/\s*notes)?$", na=False)
    )
    return df[~is_header].reset_index(drop=True)

# ✅ FIXED: merge wrapped multi-line rows into one logical row
def merge_multiline_rows(df):
    """
    If column_name is empty/NA/<NA>/nan, treat row as continuation of previous row.
    Appends description/comments to previous row.
    """
    if df.empty:
        return df

    def norm_cell(x):
        # IMPORTANT: handle pandas <NA>, NaN, None, etc.
        if x is None or pd.isna(x):
            return ""
        s = str(x).strip()
        if s.lower() in ("<na>", "nan", "none", ""):
            return ""
        return s

    merged = []
    current = None

    for _, r in df.iterrows():
        c0 = norm_cell(r["column_name"])
        c1 = norm_cell(r["description"])
        c2 = norm_cell(r["comments"])

        if c0:  # new logical row
            if current is not None:
                merged.append(current)
            current = {"column_name": c0, "description": c1, "comments": c2}
        else:
            # continuation row
            if current is None:
                # weird case: first row has empty column_name; keep it
                current = {"column_name": "", "description": c1, "comments": c2}
            else:
                if c1:
                    current["description"] = (current["description"] + " " + c1).strip() if current["description"] else c1
                if c2:
                    current["comments"] = (current["comments"] + " " + c2).strip() if current["comments"] else c2

    if current is not None:
        merged.append(current)

    return pd.DataFrame(merged)

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

def labels_on_page(lines):
    out = []
    for ln in lines:
        m = DB_RE.search(ln["text"])
        if m:
            out.append((float(ln["top"]), normalize_db_label(m)))
    out.sort(key=lambda x: x[0])
    return out

def find_label_near_table(lines, table_top, max_scan=700):
    if table_top is None:
        return None
    best_y, best_label = None, None
    for ln in lines:
        y = float(ln["top"])
        if y < table_top and (table_top - y) <= max_scan:
            m = DB_RE.search(ln["text"])
            if m and (best_y is None or y > best_y):
                best_y, best_label = y, normalize_db_label(m)
    return best_label

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
all_parts = []
active_label = ""

with pdfplumber.open(PDF_PATH) as pdf:
    total_pages = len(pdf.pages)
    end = min(END_PAGE, total_pages)

    for page_num in range(START_PAGE, end + 1):
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

        page_lines = extract_lines_with_y(pdf.pages[page_num - 1])
        page_labels = labels_on_page(page_lines)

        # Case: page has label but no tables -> label applies going forward
        if (not tables_json) and page_labels:
            active_label = page_labels[-1][1]
            continue

        if not tables_json:
            continue

        tables_json = sorted(tables_json, key=lambda t: safe_float(t.get("top")) or 1e9)

        last_table_bottom = None

        for t in tables_json:
            top = safe_float(t.get("top"))
            height = safe_float(t.get("height"))
            bottom = (top + height) if (top is not None and height is not None) else None
            if bottom is not None:
                last_table_bottom = bottom if last_table_bottom is None else max(last_table_bottom, bottom)

            # New table start if label is above THIS table
            label_above = find_label_near_table(page_lines, top, max_scan=700)
            if label_above:
                active_label = label_above

            df = tabula_json_to_df(t)
            if df.empty:
                continue

            df = normalize_3col(df)
            if df is None:
                continue

            # Source-level clean
            df = remove_table_header_rows(df)
            if df.empty:
                continue

            # ✅ multiline fix
            df = merge_multiline_rows(df)
            if df.empty:
                continue

            df.insert(0, "table_name", active_label)
            df.insert(1, "__page__", page_num)
            all_parts.append(df)

        # label below last table applies to next page (Case2)
        if page_labels:
            if last_table_bottom is not None:
                labels_below = [lbl for (y, lbl) in page_labels if y > last_table_bottom]
                if labels_below:
                    active_label = labels_below[-1]
            else:
                active_label = page_labels[-1][1]

combined = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()
display(combined)

# combined.to_csv("final_output.csv", index=False)
