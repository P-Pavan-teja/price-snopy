import re
import tabula
import pdfplumber
import pandas as pd

PDF_PATH   = "databases/Databases.pdf"
START_PAGE = 1
END_PAGE   = 300  # set your internal pages

# Extract only Abc.bac.dbc from: Abc.bac.dbc table / "Abc.bac.dbc table"
DB_RE = re.compile(r'(?i)\b([A-Za-z_][\w$]*\.[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)\b')

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def tabula_json_to_df(tjson: dict) -> pd.DataFrame:
    data = tjson.get("data") or []
    rows = [[cell.get("text", "") if isinstance(cell, dict) else "" for cell in row] for row in data]
    df = pd.DataFrame(rows)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").reset_index(drop=True)
    return df

def normalize_3col(df: pd.DataFrame):
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

def labels_on_page(lines):
    """
    Return all labels on the page with their y position, sorted top->bottom.
    """
    out = []
    for ln in lines:
        m = DB_RE.search(ln["text"])
        if m:
            out.append((float(ln["top"]), m.group(1)))
    out.sort(key=lambda x: x[0])
    return out

def find_label_near_table(lines, table_top, max_scan=700):
    """
    Find closest label ABOVE this table within max_scan.
    """
    if table_top is None:
        return None
    best_y, best_label = None, None
    for ln in lines:
        y = float(ln["top"])
        if y < table_top and (table_top - y) <= max_scan:
            m = DB_RE.search(ln["text"])
            if m and (best_y is None or y > best_y):
                best_y, best_label = y, m.group(1)
    return best_label

# ----------------------------
# Main logic
# ----------------------------
all_parts = []
active_label = ""  # current db.sch.tbl context

with pdfplumber.open(PDF_PATH) as pdf:
    total_pages = len(pdf.pages)
    end = min(END_PAGE, total_pages)

    for page_num in range(START_PAGE, end + 1):
        # Read tables on this page
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

        # Extract page text lines once
        page_lines = extract_lines_with_y(pdf.pages[page_num - 1], y_tol=3)
        page_labels = labels_on_page(page_lines)  # [(y,label), ...]

        # If page has NO tables but has labels: the last label should apply going forward
        if (not tables_json) and page_labels:
            active_label = page_labels[-1][1]
            continue

        if not tables_json:
            continue

        # Sort tables top->bottom so state is consistent
        def table_top(t): return safe_float(t.get("top")) or 1e9
        tables_json = sorted(tables_json, key=table_top)

        last_table_bottom = None

        for t in tables_json:
            top = safe_float(t.get("top"))
            height = safe_float(t.get("height"))
            bottom = (top + height) if (top is not None and height is not None) else None
            if bottom is not None:
                last_table_bottom = bottom if last_table_bottom is None else max(last_table_bottom, bottom)

            # Case1 (normal/new table): label above THIS table
            label_above = find_label_near_table(page_lines, top, max_scan=700)
            if label_above:
                active_label = label_above  # new context starts here

            # If no label above, treat as continuation: keep current active_label
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

        # ---- Case2 fix: label appears AFTER the last table on the page ----
        # Example: Page2 ends tbl1, later shows db1.sch.tbl2 -> should apply to Page3.
        if page_labels:
            # if we have last_table_bottom, only consider labels below it
            if last_table_bottom is not None:
                labels_below = [lbl for (y, lbl) in page_labels if y > last_table_bottom]
                if labels_below:
                    active_label = labels_below[-1]  # last label below last table
            else:
                # if we couldn't compute bottom, safest: take last label on page
                active_label = page_labels[-1][1]

combined = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()
display(combined)

# combined.to_csv("tables_with_table_name.csv", index=False)
