import fnmatch
import tarfile
from io import BytesIO
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Font
from openpyxl.utils import get_column_letter


# ==========================================================
# Base directory (works even if you run from elsewhere)
# ==========================================================
BASE_DIR = Path(__file__).resolve().parent
AFTER_DIR = BASE_DIR / "After_Run"
BEFORE_DIR = BASE_DIR / "Before_Run"

# ==========================================================
# Styling configuration
# ==========================================================
LIGHT_GREEN = PatternFill("solid", fgColor="C6EFCE")   # match
LIGHT_YELLOW = PatternFill("solid", fgColor="FFF2CC")  # mismatch
THIN_SIDE = Side(style="thin", color="000000")
THIN_BORDER = Border(left=THIN_SIDE, right=THIN_SIDE, top=THIN_SIDE, bottom=THIN_SIDE)

HEADER_FONT = Font(bold=True)
TITLE_FONT = Font(bold=True)
CENTER = Alignment(horizontal="center", vertical="center")

# Excel layout
TOP_TITLE_ROW = 1      # After_Run Results / Before_Run Results
SUB_TITLE_ROW = 3      # After Run - Facility ... / Before Run - Facility ...
TABLE_START_ROW = 5    # pivot header row starts here

AFTER_START_COL = 1    # column A
GAP_COLS = 2           # 2-column gap


# ==========================================================
# TAR/PSV extraction helpers
# ==========================================================
def find_first_matching_tar(folder: Path, pattern: str) -> Path | None:
    """Return the first matching tar path under folder for given pattern e.g. 'cnb_in_out_*.tar'."""
    matches = sorted(folder.glob(pattern))
    return matches[0] if matches else None


def extract_inner_tar_bytes(outer_tar: tarfile.TarFile, inner_tar_glob: str) -> BytesIO | None:
    """
    From an open outer tarfile, locate an inner tar member matching inner_tar_glob,
    extract into BytesIO, and return it.
    """
    candidates = [
        m for m in outer_tar.getmembers()
        if fnmatch.fnmatch(Path(m.name).name, inner_tar_glob)
    ]
    if not candidates:
        return None

    inner_member = candidates[0]
    f = outer_tar.extractfile(inner_member)
    if f is None:
        return None

    return BytesIO(f.read())


def read_psv_from_inner_tar(inner_tar_bytes: BytesIO, psv_glob: str) -> pd.DataFrame | None:
    """
    Open inner tar from BytesIO and read first PSV that matches psv_glob into a DataFrame.
    """
    inner_tar_bytes.seek(0)
    with tarfile.open(fileobj=inner_tar_bytes, mode="r:*") as itar:
        members = itar.getmembers()
        psv_candidates = [
            m for m in members
            if fnmatch.fnmatch(Path(m.name).name, psv_glob)
        ]
        if not psv_candidates:
            return None

        psv_member = psv_candidates[0]
        f = itar.extractfile(psv_member)
        if f is None:
            return None

        # PSV uses pipe delimiter and already has header
        return pd.read_csv(f, sep="|", engine="python")


def load_facility_df(folder: Path, outer_tar_pattern: str, inner_tar_pattern: str, psv_pattern: str) -> pd.DataFrame | None:
    """
    Find outer tar, open it, extract inner tar bytes, read PSV from inner tar -> dataframe.
    """
    outer_tar_path = find_first_matching_tar(folder, outer_tar_pattern)
    if not outer_tar_path:
        return None

    with tarfile.open(outer_tar_path, mode="r:*") as otar:
        inner_bytes = extract_inner_tar_bytes(otar, inner_tar_pattern)
        if inner_bytes is None:
            return None
        return read_psv_from_inner_tar(inner_bytes, psv_pattern)


def load_cms_facility_df(folder: Path) -> pd.DataFrame | None:
    """
    For commercial case, facility_cms_out_*.psv can be inside cms_out_*.tar OR esn_out_*.tar.
    Try cms_out first, then esn_out.
    """
    outer_tar_path = find_first_matching_tar(folder, "lgd_commercial_in_out_*.tar")
    if not outer_tar_path:
        return None

    with tarfile.open(outer_tar_path, mode="r:*") as otar:
        for inner_tar_pattern in ["cms_out_*.tar", "esn_out_*.tar"]:
            inner_bytes = extract_inner_tar_bytes(otar, inner_tar_pattern)
            if inner_bytes is None:
                continue
            df = read_psv_from_inner_tar(inner_bytes, "facility_cms_out_*.psv")
            if df is not None:
                return df

    return None


# ==========================================================
# Pivot logic
# ==========================================================
def normalize_lgd_rate(series: pd.Series) -> pd.Series:
    """
    Convert FinalLGDRate to numeric and normalize to fraction (0-1) if it looks like 0-100.
    """
    s = pd.to_numeric(series, errors="coerce")
    non_na = s.dropna()
    if not non_na.empty:
        mx = non_na.max()
        if mx > 1.0 and mx <= 100.0:
            s = s / 100.0
    return s


def make_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Output pivot dataframe:
      - Final Segment ID
      - Count of FacilityID
      - Average Final LGD Rate (stored as fraction, formatted as % in Excel)
    """
    required = {"FacilityID", "FinalSegmentID", "FinalLGDRate"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    tmp = df[["FacilityID", "FinalSegmentID", "FinalLGDRate"]].copy()
    tmp["FinalLGDRate"] = normalize_lgd_rate(tmp["FinalLGDRate"])

    pt = (
        tmp.groupby("FinalSegmentID", dropna=False)
           .agg(
               **{
                   "Count of FacilityID": ("FacilityID", "count"),
                   "Average Final LGD Rate": ("FinalLGDRate", "mean"),
               }
           )
           .reset_index()
           .rename(columns={"FinalSegmentID": "Final Segment ID"})
    )

    # Sort by Final Segment ID if numeric-like
    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return float("inf")

    pt["_k"] = pt["Final Segment ID"].apply(safe_float)
    pt = pt.sort_values("_k").drop(columns="_k")

    return pt


def align_pivots(after_pt: pd.DataFrame, before_pt: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ensure both pivots have the same set of Final Segment IDs (outer union),
    so comparison/highlighting is row-aligned.
    """
    a = after_pt.set_index("Final Segment ID")
    b = before_pt.set_index("Final Segment ID")

    all_idx = a.index.union(b.index)
    a2 = a.reindex(all_idx).reset_index()
    b2 = b.reindex(all_idx).reset_index()
    return a2, b2


# ==========================================================
# Excel writer + formatting + comparison highlighting
# ==========================================================
def write_side_by_side_excel(
    out_xlsx: Path,
    sheet_name: str,
    after_pt: pd.DataFrame,
    before_pt: pd.DataFrame,
    after_title: str,
    before_title: str
):
    after_aligned, before_aligned = align_pivots(after_pt, before_pt)

    after_width = after_aligned.shape[1]              # 3 columns
    before_start_col = AFTER_START_COL + after_width + GAP_COLS

    # Write both tables
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)

        after_aligned.to_excel(
            writer, sheet_name=sheet_name,
            index=False, startrow=TABLE_START_ROW - 1, startcol=AFTER_START_COL - 1
        )
        before_aligned.to_excel(
            writer, sheet_name=sheet_name,
            index=False, startrow=TABLE_START_ROW - 1, startcol=before_start_col - 1
        )

    # Load workbook to apply formatting
    wb = load_workbook(out_xlsx)
    ws = wb[sheet_name]

    # Titles
    ws.cell(row=TOP_TITLE_ROW, column=AFTER_START_COL, value="After_Run Results").font = TITLE_FONT
    ws.cell(row=TOP_TITLE_ROW, column=before_start_col, value="Before_Run Results").font = TITLE_FONT

    ws.cell(row=SUB_TITLE_ROW, column=AFTER_START_COL, value=after_title).font = TITLE_FONT
    ws.cell(row=SUB_TITLE_ROW, column=before_start_col, value=before_title).font = TITLE_FONT

    # Header formatting
    header_row = TABLE_START_ROW
    for c in range(AFTER_START_COL, AFTER_START_COL + after_width):
        ws.cell(row=header_row, column=c).font = HEADER_FONT
        ws.cell(row=header_row, column=c).alignment = CENTER

    for c in range(before_start_col, before_start_col + after_width):
        ws.cell(row=header_row, column=c).font = HEADER_FONT
        ws.cell(row=header_row, column=c).alignment = CENTER

    # Table bounds
    n_rows = after_aligned.shape[0]
    total_rows = n_rows + 1  # header included

    after_r1, after_r2 = header_row, header_row + total_rows - 1
    after_c1, after_c2 = AFTER_START_COL, AFTER_START_COL + after_width - 1

    before_r1, before_r2 = header_row, header_row + total_rows - 1
    before_c1, before_c2 = before_start_col, before_start_col + after_width - 1

    # Apply percent format to Avg column (3rd column)
    avg_offset = 2
    for r in range(header_row + 1, after_r2 + 1):
        ws.cell(row=r, column=after_c1 + avg_offset).number_format = "0%"
        ws.cell(row=r, column=before_c1 + avg_offset).number_format = "0%"

    # Borders
    def apply_border(r1, r2, c1, c2):
        for rr in range(r1, r2 + 1):
            for cc in range(c1, c2 + 1):
                ws.cell(row=rr, column=cc).border = THIN_BORDER

    apply_border(after_r1, after_r2, after_c1, after_c2)
    apply_border(before_r1, before_r2, before_c1, before_c2)

    # Auto-size columns (simple)
    def autosize(c1, c2, last_row):
        for cc in range(c1, c2 + 1):
            max_len = 0
            for rr in range(1, last_row + 1):
                v = ws.cell(row=rr, column=cc).value
                if v is None:
                    continue
                max_len = max(max_len, len(str(v)))
            ws.column_dimensions[get_column_letter(cc)].width = min(max(12, max_len + 2), 45)

    autosize(after_c1, after_c2, after_r2)
    autosize(before_c1, before_c2, before_r2)

    # Comparison highlighting (match => green, mismatch => yellow)
    tol = 1e-9

    def match(v1, v2, is_avg=False):
        if v1 is None and v2 is None:
            return True
        if (v1 is None) != (v2 is None):
            return False
        try:
            if pd.isna(v1) and pd.isna(v2):
                return True
            if pd.isna(v1) != pd.isna(v2):
                return False
        except Exception:
            pass

        if is_avg:
            try:
                return abs(float(v1) - float(v2)) <= tol
            except Exception:
                return str(v1) == str(v2)
        return str(v1) == str(v2)

    for i in range(n_rows):
        rr = header_row + 1 + i
        for j in range(after_width):
            a_cell = ws.cell(row=rr, column=after_c1 + j)
            b_cell = ws.cell(row=rr, column=before_c1 + j)

            is_avg = (j == avg_offset)
            is_match = match(a_cell.value, b_cell.value, is_avg=is_avg)
            fill = LIGHT_GREEN if is_match else LIGHT_YELLOW

            a_cell.fill = fill
            b_cell.fill = fill

    wb.save(out_xlsx)


# ==========================================================
# Main orchestration (presence rules + generate files)
# ==========================================================
def main(after_dir: Path, before_dir: Path):
    # Presence checks (outer tars)
    cnb_after = find_first_matching_tar(after_dir, "cnb_in_out_*.tar")
    cnb_before = find_first_matching_tar(before_dir, "cnb_in_out_*.tar")

    ccms_after = find_first_matching_tar(after_dir, "lgd_ccms_in_out_*.tar")
    ccms_before = find_first_matching_tar(before_dir, "lgd_ccms_in_out_*.tar")

    cms_after = find_first_matching_tar(after_dir, "lgd_commercial_in_out_*.tar")
    cms_before = find_first_matching_tar(before_dir, "lgd_commercial_in_out_*.tar")

    # -------------------- CNB --------------------
    if cnb_after and cnb_before:
        df_ar = load_facility_df(after_dir, "cnb_in_out_*.tar", "cnb_out_*.tar", "facility_cnb_out_*.psv")
        df_br = load_facility_df(before_dir, "cnb_in_out_*.tar", "cnb_out_*.tar", "facility_cnb_out_*.psv")

        if df_ar is None or df_br is None:
            print("CNB: inner TAR or PSV not found in After_Run or Before_Run. CNB file not generated.")
        else:
            ar_pt = make_pivot(df_ar)
            br_pt = make_pivot(df_br)

            write_side_by_side_excel(
                out_xlsx=BASE_DIR / "CNB_Facility_Validation.xlsx",
                sheet_name="CNB_Facility_Validation",
                after_pt=ar_pt,
                before_pt=br_pt,
                after_title="After Run - Facility CNB Out",
                before_title="Before Run - Facility CNB Out",
            )
            print("Generated: CNB_Facility_Validation.xlsx")
    else:
        if not cnb_after:
            print("cnb_in_out_*.tar is absent in After_Run Folder so CNB_Facility_Validation.xlsx is not generated.")
        if not cnb_before:
            print("cnb_in_out_*.tar is absent in Before_Run Folder so CNB_Facility_Validation.xlsx is not generated.")

    # -------------------- CCMS --------------------
    if ccms_after and ccms_before:
        df_ar = load_facility_df(after_dir, "lgd_ccms_in_out_*.tar", "ccms_out_*.tar", "facility_ccms_out_*.psv")
        df_br = load_facility_df(before_dir, "lgd_ccms_in_out_*.tar", "ccms_out_*.tar", "facility_ccms_out_*.psv")

        if df_ar is None or df_br is None:
            print("CCMS: inner TAR or PSV not found in After_Run or Before_Run. CCMS file not generated.")
        else:
            ar_pt = make_pivot(df_ar)
            br_pt = make_pivot(df_br)

            write_side_by_side_excel(
                out_xlsx=BASE_DIR / "CCMS_Facility_Validation.xlsx",
                sheet_name="CCMS_Facility_Validation",
                after_pt=ar_pt,
                before_pt=br_pt,
                after_title="After Run - Facility CCMS Out",
                before_title="Before Run - Facility CCMS Out",
            )
            print("Generated: CCMS_Facility_Validation.xlsx")
    else:
        if not ccms_after:
            print("lgd_ccms_in_out_*.tar is absent in After_Run Folder so CCMS_Facility_Validation.xlsx is not generated.")
        if not ccms_before:
            print("lgd_ccms_in_out_*.tar is absent in Before_Run Folder so CCMS_Facility_Validation.xlsx is not generated.")

    # -------------------- CMS --------------------
    if cms_after and cms_before:
        df_ar = load_cms_facility_df(after_dir)
        df_br = load_cms_facility_df(before_dir)

        if df_ar is None or df_br is None:
            print("CMS: cms_out/esn_out TAR or facility_cms_out PSV not found. CMS file not generated.")
        else:
            ar_pt = make_pivot(df_ar)
            br_pt = make_pivot(df_br)

            write_side_by_side_excel(
                out_xlsx=BASE_DIR / "CMS_Facility_Validation.xlsx",
                sheet_name="CMS_Facility_Validation",
                after_pt=ar_pt,
                before_pt=br_pt,
                after_title="After Run - Facility CMS Out",
                before_title="Before Run - Facility CMS Out",
            )
            print("Generated: CMS_Facility_Validation.xlsx")
    else:
        if not cms_after:
            print("lgd_commercial_in_out_*.tar is absent in After_Run Folder so CMS_Facility_Validation.xlsx is not generated.")
        if not cms_before:
            print("lgd_commercial_in_out_*.tar is absent in Before_Run Folder so CMS_Facility_Validation.xlsx is not generated.")


if __name__ == "__main__":
    main(AFTER_DIR, BEFORE_DIR)