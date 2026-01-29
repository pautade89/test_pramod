import tarfile
import tempfile
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter


# =========================
# Context 2 (Dynamic patterns)
# =========================
cnb_list = [
    "error_summary_cnb_out_*.psv",
    "summary_count_cnb_out_*.psv",
]

ccms_list = [
    "error_summary_ccms_out_*.psv",
    "summary_ccms_out_*.psv",
    "summary_count_ccms_out_*.psv",
]

cms_list = [
    "error_summary_cms_out_*.psv",
    "summary_cms_out_*.psv",
    "summary_count_cms_out_*.psv",
]


# =========================
# Folder + TAR Patterns (Context 1)
# =========================
AFTER_DIR = Path("After_Run")
BEFORE_DIR = Path("Before_Run")

CNB_OUTER_PATTERN = "cnb_in_out_*.tar"
CNB_INNER_OUT_PATTERN = "cnb_out_*.tar"

CCMS_OUTER_PATTERN = "lgd_ccms_in_out_*.tar"
CCMS_INNER_OUT_PATTERN = "ccms_out_*.tar"

COMM_OUTER_PATTERN = "lgd_commercial_in_out_*.tar"
CMS_INNER_OUT_PATTERN = "cms_out_*.tar"
ESN_INNER_OUT_PATTERN = "esn_out_*.tar"


# =========================
# Excel Layout (Context 5/6)
# =========================
ROW_GAP_BETWEEN_TABLES = 3
COL_GAP_BETWEEN_BLOCKS = 2  # exactly 2 blank columns between After and Before

TITLE_ROW = 1
FIRST_SECTION_TITLE_ROW = 3  # similar to your reference screenshot

# Highlight fill for differences (Context 7)
DIFF_FILL = PatternFill("solid", fgColor="FFF2CC")  # light yellow
TITLE_FONT = Font(bold=True, size=12)
BOLD = Font(bold=True)
ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=False)


# =========================
# Helper: TAR + File Search
# =========================
def extract_tar(tar_path: Path, extract_to: Path) -> None:
    with tarfile.open(tar_path, "r:*") as tar:
        tar.extractall(path=extract_to)


def pick_one(paths, what: str) -> Path:
    if not paths:
        raise FileNotFoundError(f"Could not find {what}")
    paths = sorted(paths)
    if len(paths) > 1:
        print(f"⚠️ Multiple matches for {what}. Using: {paths[0].name}")
    return paths[0]


def find_in_dir_by_glob(root: Path, pattern: str):
    # recursive glob search
    return sorted(root.rglob(pattern))


def find_in_top_by_glob(root: Path, pattern: str):
    # search only immediate directory
    return sorted(root.glob(pattern))


def read_psv_to_df(psv_path: Path) -> pd.DataFrame:
    # "remove '|' symbol as delimiter" => use '|' as delimiter
    # Keep rows/cols as-is: read as strings, do not convert NaN aggressively
    return pd.read_csv(
        psv_path,
        sep="|",
        dtype=str,
        keep_default_na=False,
        engine="python"
    )


def extract_outer_then_inner(base_folder: Path, outer_pattern: str, inner_patterns):
    """
    Extract an outer tar (matched by outer_pattern) from base_folder,
    then extract one or more inner tar(s) (matched by inner_patterns) from within it.
    Returns:
      tmpdir_handle, inner_extract_root (Path)
    """
    outer_matches = find_in_top_by_glob(base_folder, outer_pattern)
    outer_tar = pick_one(outer_matches, f"outer tar '{outer_pattern}' in {base_folder}")

    tmpdir = tempfile.TemporaryDirectory()
    tmp_root = Path(tmpdir.name)

    outer_extract = tmp_root / "outer"
    outer_extract.mkdir(parents=True, exist_ok=True)
    extract_tar(outer_tar, outer_extract)

    inner_extract_root = tmp_root / "inner"
    inner_extract_root.mkdir(parents=True, exist_ok=True)

    if isinstance(inner_patterns, str):
        inner_patterns = [inner_patterns]

    for pat in inner_patterns:
        inner_matches = find_in_dir_by_glob(outer_extract, pat)
        inner_tar = pick_one(inner_matches, f"inner tar '{pat}' inside {outer_tar.name}")

        dest = inner_extract_root / inner_tar.stem
        dest.mkdir(parents=True, exist_ok=True)
        extract_tar(inner_tar, dest)

    return tmpdir, inner_extract_root


def load_psv_dfs_from_run(run_folder: Path, outer_pattern: str, inner_patterns, psv_patterns):
    """
    Generic loader:
    - extract outer tar (pattern)
    - extract inner tar(s) (pattern(s))
    - search each requested PSV pattern inside extracted inner content
    - read each into DataFrame
    """
    tmpdir, root = extract_outer_then_inner(run_folder, outer_pattern, inner_patterns)
    try:
        dfs = []
        for psv_pat in psv_patterns:
            matches = find_in_dir_by_glob(root, psv_pat)
            psv_file = pick_one(matches, f"psv '{psv_pat}' inside extracted content of {run_folder}")
            dfs.append(read_psv_to_df(psv_file))
        return dfs
    finally:
        tmpdir.cleanup()


# =========================
# Excel Writing + Diff Highlight
# =========================
def autosize_columns(ws, col_start: int, col_end: int, row_start: int, row_end: int, min_w=10, max_w=60):
    for c in range(col_start, col_end + 1):
        max_len = 0
        for r in range(row_start, row_end + 1):
            v = ws.cell(r, c).value
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[get_column_letter(c)].width = min(max(min_w, max_len + 2), max_w)


def write_title(ws, row: int, col: int, text: str, span_cols: int):
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = TITLE_FONT
    cell.alignment = ALIGN_LEFT
    if span_cols > 1:
        ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + span_cols - 1)


def write_section_label(ws, row: int, col: int, text: str):
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = BOLD
    cell.alignment = ALIGN_LEFT


def write_df(ws, df: pd.DataFrame, start_row: int, start_col: int):
    """
    Write df with header at start_row.
    Returns a range tuple: (top, left, bottom, right) INCLUDING header row.
    """
    nrows, ncols = df.shape

    # Header
    for j, name in enumerate(df.columns):
        cell = ws.cell(row=start_row, column=start_col + j, value=str(name))
        cell.font = BOLD
        cell.alignment = ALIGN_LEFT

    # Data
    for i in range(nrows):
        for j in range(ncols):
            ws.cell(row=start_row + 1 + i, column=start_col + j, value=df.iat[i, j])

    top = start_row
    left = start_col
    bottom = start_row + nrows  # header + nrows
    right = start_col + ncols - 1
    return top, left, bottom, right


def compare_and_highlight(ws, rng_after, rng_before):
    """
    Compare two written table ranges (including header).
    Highlight cells (both sides) in light yellow if values differ.
    """
    a_top, a_left, a_bottom, a_right = rng_after
    b_top, b_left, b_bottom, b_right = rng_before

    a_rows = a_bottom - a_top + 1
    a_cols = a_right - a_left + 1
    b_rows = b_bottom - b_top + 1
    b_cols = b_right - b_left + 1

    max_rows = max(a_rows, b_rows)
    max_cols = max(a_cols, b_cols)

    for r in range(max_rows):
        for c in range(max_cols):
            a_r = a_top + r
            a_c = a_left + c
            b_r = b_top + r
            b_c = b_left + c

            a_exists = (r < a_rows and c < a_cols)
            b_exists = (r < b_rows and c < b_cols)

            a_val = ws.cell(a_r, a_c).value if a_exists else ""
            b_val = ws.cell(b_r, b_c).value if b_exists else ""

            a_str = "" if a_val is None else str(a_val)
            b_str = "" if b_val is None else str(b_val)

            if a_str != b_str:
                if a_exists:
                    ws.cell(a_r, a_c).fill = DIFF_FILL
                if b_exists:
                    ws.cell(b_r, b_c).fill = DIFF_FILL


def build_validation_excel(
    out_file: str,
    sheet_name: str,
    after_tables: list,
    before_tables: list,
    after_titles: list,
    before_titles: list,
    after_block_title: str = "After_Run Results",
    before_block_title: str = "Before_Run Results",
):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    # block width controls where Before block starts
    after_max_cols = max(df.shape[1] for df in after_tables) if after_tables else 1
    before_max_cols = max(df.shape[1] for df in before_tables) if before_tables else 1
    block_width = max(after_max_cols, before_max_cols)

    left_start_col = 1  # A
    # EXACT: if after ends at G (7), before starts at J (10) => right_start = 1 + 7 + 2
    right_start_col = left_start_col + block_width + COL_GAP_BETWEEN_BLOCKS

    # Titles
    write_title(ws, TITLE_ROW, left_start_col, after_block_title, block_width)
    write_title(ws, TITLE_ROW, right_start_col, before_block_title, block_width)

    # Write tables
    after_ranges = []
    before_ranges = []

    cur_after_label_row = FIRST_SECTION_TITLE_ROW
    cur_before_label_row = FIRST_SECTION_TITLE_ROW

    for i, df in enumerate(after_tables):
        write_section_label(ws, cur_after_label_row, left_start_col, after_titles[i])
        table_start = cur_after_label_row + 1
        rng = write_df(ws, df, table_start, left_start_col)
        after_ranges.append(rng)
        cur_after_label_row = rng[2] + 1 + ROW_GAP_BETWEEN_TABLES

    for i, df in enumerate(before_tables):
        write_section_label(ws, cur_before_label_row, right_start_col, before_titles[i])
        table_start = cur_before_label_row + 1
        rng = write_df(ws, df, table_start, right_start_col)
        before_ranges.append(rng)
        cur_before_label_row = rng[2] + 1 + ROW_GAP_BETWEEN_TABLES

    # Context 7: compare corresponding tables and highlight diffs
    for i in range(min(len(after_ranges), len(before_ranges))):
        compare_and_highlight(ws, after_ranges[i], before_ranges[i])

    # Autosize columns (no table coloring; only diff highlights apply)
    last_row = max(
        after_ranges[-1][2] if after_ranges else 1,
        before_ranges[-1][2] if before_ranges else 1,
    )
    autosize_columns(ws, left_start_col, left_start_col + block_width - 1, 1, last_row)
    autosize_columns(ws, right_start_col, right_start_col + block_width - 1, 1, last_row)

    wb.save(out_file)


# =========================
# Main Execution
# =========================
def main():
    # ---------------- CNB ----------------
    # After_Run
    df_ar_cnb_es, df_ar_cnb_sc = load_psv_dfs_from_run(
        AFTER_DIR, CNB_OUTER_PATTERN, CNB_INNER_OUT_PATTERN, cnb_list
    )
    # Before_Run
    df_br_cnb_es, df_br_cnb_sc = load_psv_dfs_from_run(
        BEFORE_DIR, CNB_OUTER_PATTERN, CNB_INNER_OUT_PATTERN, cnb_list
    )

    build_validation_excel(
        out_file="CNB_Validation.excel",
        sheet_name="CNB_Validation",
        after_tables=[df_ar_cnb_es, df_ar_cnb_sc],
        before_tables=[df_br_cnb_es, df_br_cnb_sc],
        after_titles=[
            "After Run - Error Summary CNB Out (File Name)",
            "After Run - Summary Count CNB Out (File Name)",
        ],
        before_titles=[
            "Before Run - Error Summary CNB Out (File Name)",
            "Before Run - Summary Count CNB Out (File Name)",
        ],
    )

    # ---------------- CCMS ----------------
    df_ar_ccms_es, df_ar_ccms_sc, df_ar_ccms_scc = load_psv_dfs_from_run(
        AFTER_DIR, CCMS_OUTER_PATTERN, CCMS_INNER_OUT_PATTERN, ccms_list
    )
    df_br_ccms_es, df_br_ccms_sc, df_br_ccms_scc = load_psv_dfs_from_run(
        BEFORE_DIR, CCMS_OUTER_PATTERN, CCMS_INNER_OUT_PATTERN, ccms_list
    )

    build_validation_excel(
        out_file="CCMS_Validation.excel",
        sheet_name="CCMS_Validation",
        after_tables=[df_ar_ccms_es, df_ar_ccms_sc, df_ar_ccms_scc],
        before_tables=[df_br_ccms_es, df_br_ccms_sc, df_br_ccms_scc],
        after_titles=[
            "After Run - Error Summary CCMS Out (File Name)",
            "After Run - Summary CCMS Out (File Name)",
            "After Run - Summary Count CCMS Out (File Name)",
        ],
        before_titles=[
            "Before Run - Error Summary CCMS Out (File Name)",
            "Before Run - Summary CCMS Out (File Name)",
            "Before Run - Summary Count CCMS Out (File Name)",
        ],
    )

    # ---------------- CMS (Commercial) ----------------
    # Extract both cms_out_*.tar and esn_out_*.tar (search PSV across both extracted trees)
    df_ar_cms_es, df_ar_cms_sc, df_ar_cms_scc = load_psv_dfs_from_run(
        AFTER_DIR, COMM_OUTER_PATTERN, [CMS_INNER_OUT_PATTERN, ESN_INNER_OUT_PATTERN], cms_list
    )
    df_br_cms_es, df_br_cms_sc, df_br_cms_scc = load_psv_dfs_from_run(
        BEFORE_DIR, COMM_OUTER_PATTERN, [CMS_INNER_OUT_PATTERN, ESN_INNER_OUT_PATTERN], cms_list
    )

    build_validation_excel(
        out_file="CMS_Validation.excel",
        sheet_name="CMS_Validation",
        after_tables=[df_ar_cms_es, df_ar_cms_sc, df_ar_cms_scc],
        before_tables=[df_br_cms_es, df_br_cms_sc, df_br_cms_scc],
        after_titles=[
            "After Run - Error Summary CMS Out (File Name)",
            "After Run - Summary CMS Out (File Name)",
            "After Run - Summary Count CMS Out (File Name)",
        ],
        before_titles=[
            "Before Run - Error Summary CMS Out (File Name)",
            "Before Run - Summary CMS Out (File Name)",
            "Before Run - Summary Count CMS Out (File Name)",
        ],
    )

    print("✅ Created files:")
    print(" - CNB_Validation.excel")
    print(" - CCMS_Validation.excel")
    print(" - CMS_Validation.excel")
    print("✅ Differences highlighted in light yellow (only diff cells; no other table coloring).")


if __name__ == "__main__":
    main()
``