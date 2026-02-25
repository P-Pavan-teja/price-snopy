import re
import tabula
import pdfplumber
import pandas as pd

PDF_PATH   = "databases/Databases.pdf"
START_PAGE = 1
END_PAGE   = 300   # change as needed

DB_RE = re.compile(r'(?i)\b([A-Za-z_][\w$]*\.[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)\b')

# ------------------------------------------------------------

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def tabula_json_to_df(tjson):
    data = tjson.get("data") or []
    rows = [[cell.get("text", "") if isinstance(cell, dict) else "" for cell in row] for row in data]
    df = pd.DataFrame(rows)

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

# ✅ REMOVE REPEATED HEADER ROWS
def remove_header_rows(df):
    return df[
        ~(
            df["column_name"].str.lower().eq("column") &
            df["description"].str.lower().eq("description") &
            df["comments"].str.lower().eq("comments")
        )
    ].reset_index(drop=True)

# ------------------------------------------------------------

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
            out.append((float(ln["top"]), m.group(1)))

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
                best_y, best_label = y, m.group(1)

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

        # Case: page has label but no table → handoff to next page
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
            bottom = (top + height) if (top and height) else None

            if bottom:
                last_table_bottom = bottom if last_table_bottom is None else max(last_table_bottom, bottom)

            # 🔹 NEW TABLE START
            label_above = find_label_near_table(page_lines, top)

            if label_above:
                active_label = label_above

            df = tabula_json_to_df(t)
            if df.empty:
                continue

            df = normalize_3col(df)
            if df is None:
                continue

            df = remove_header_rows(df)   # ✅ HEADER REMOVAL

            df.insert(0, "table_name", active_label)
            df.insert(1, "__page__", page_num)

            all_parts.append(df)

        # 🔹 LABEL BELOW LAST TABLE → APPLY TO NEXT PAGE
        if page_labels and last_table_bottom:
            labels_below = [lbl for (y, lbl) in page_labels if y > last_table_bottom]
            if labels_below:
                active_label = labels_below[-1]

# ------------------------------------------------------------

combined = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()

display(combined)

# combined.to_csv("final_output.csv", index=False)
