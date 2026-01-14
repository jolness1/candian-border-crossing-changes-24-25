import csv
import os
from collections import defaultdict, OrderedDict
from typing import Dict, List


INPUT_DIR = os.path.join("output", "montana-history")


def read_port_csv(path: str):
    # returns data[measure][year][month] = value
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    years_set = set()
    months = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            try:
                year = int(r["year"]) 
                month = r["month"]
                measure = r["crossingType"]
                value = int(float(r.get("numberOfCrossings", 0)))
            except Exception:
                continue
            data[measure][year][month] += value
            years_set.add(year)
            if month not in months:
                months.append(month)
    # ensure months are ordered Jan..Dec if present
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    months_sorted = [m for m in month_order if m in months]
    return data, sorted(years_set), months_sorted


def ensure_total(data: Dict):
    # compute two totals:
    # - 'Total People' includes any measure that looks like people (e.g. contains 'passeng' or 'pedestrian')
    # - 'Total Vehicles' includes everything else
    totals_people = defaultdict(lambda: defaultdict(int))
    totals_vehicles = defaultdict(lambda: defaultdict(int))
    for measure, years in list(data.items()):
        name = (measure or "").strip()
        # skip any pre-existing totals to avoid double-counting
        if name.lower().startswith("total"):
            continue
        key = name.lower()
        # treat anything that looks like people/passengers as people
        is_person = ('passeng' in key) or ('pedestrian' in key) or ('person' in key)
        for y, months in years.items():
            for m, v in months.items():
                if is_person:
                    totals_people[y][m] += v
                else:
                    totals_vehicles[y][m] += v
    # remove any old 'Total' key if present to avoid confusion
    if "Total" in data:
        del data["Total"]
    data["Total People"] = totals_people
    data["Total Vehicles"] = totals_vehicles


def write_yoy_tables(port_name: str, data: Dict, years: List[int], months: List[str], out_dir: str):
    safe = port_name
    # header years should include all years (even earliest) per user spec
    header_years = years

    abs_path = os.path.join(out_dir, f"{safe}-YoY-absolute.csv")
    pct_path = os.path.join(out_dir, f"{safe}-YoY-percent.csv")

    measures = sorted(data.keys())

    # Prepare rows: for each month and measure, produce row with changes per year
    with open(abs_path, "w", newline="", encoding="utf-8") as afh, open(pct_path, "w", newline="", encoding="utf-8") as pfh:
        awriter = csv.writer(afh)
        pwriter = csv.writer(pfh)
        awriter.writerow(["month", "crossingType"] + [str(y) for y in header_years])
        pwriter.writerow(["month", "crossingType"] + [str(y) for y in header_years])

        for month in months:
            for measure in measures:
                row_abs = [month, measure]
                row_pct = [month, measure]
                for i, y in enumerate(header_years):
                    prev = y - 1
                    curr_val = data.get(measure, {}).get(y, {}).get(month, 0)
                    prev_val = data.get(measure, {}).get(prev, {}).get(month, None)
                    if prev_val is None:
                        row_abs.append("")
                        row_pct.append("")
                    else:
                        abs_change = curr_val - prev_val
                        row_abs.append(str(abs_change))
                        if prev_val == 0:
                            row_pct.append("")
                        else:
                            pct_change = (abs_change / prev_val) * 100.0
                            # Round to 3 decimals, drop trailing zeros
                            row_pct.append(str(round(pct_change, 6)).rstrip('0').rstrip('.') if isinstance(pct_change, float) else str(pct_change))
                awriter.writerow(row_abs)
                pwriter.writerow(row_pct)


def write_yearly_summary(port_name: str, data: Dict, years: List[int], out_dir: str):
    # For each year (starting from second available), aggregate across months per measure
    path = os.path.join(out_dir, f"{port_name}-yearly.csv")
    measures = sorted(data.keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["year", "crossingType", "absoluteChange", "pctChange"])
        for y in years:
            prev = y - 1
            for measure in measures:
                # sum across months for the year
                curr_total = sum(data.get(measure, {}).get(y, {}).values())
                prev_total = sum(data.get(measure, {}).get(prev, {}).values()) if prev in data.get(measure, {}) else None
                if prev_total is None:
                    # skip or write empty changes
                    writer.writerow([y, measure, "", ""])
                else:
                    abs_change = curr_total - prev_total
                    if prev_total == 0:
                        pct = ""
                    else:
                        pct = round((abs_change / prev_total) * 100.0, 6)
                        pct = str(pct).rstrip('0').rstrip('.') if isinstance(pct, float) else str(pct)
                    writer.writerow([y, measure, abs_change, pct])


def process_all_ports(input_dir=INPUT_DIR):
    if not os.path.isdir(input_dir):
        print(f"Input dir not found: {input_dir}")
        return
    files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    for fn in files:
        path = os.path.join(input_dir, fn)
        port_name = os.path.splitext(fn)[0]
        # skip files that are the YoY or yearly outputs if present
        if port_name.endswith(('-YoY-absolute', '-YoY-percent', '-yearly')):
            continue
        data, years, months = read_port_csv(path)
        if not years:
            continue
        ensure_total(data)
        # create per-port analysis folder under ./output/{Port Name}-analysis
        port_out_dir = os.path.join("output", f"{port_name}-analysis")
        os.makedirs(port_out_dir, exist_ok=True)
        write_yoy_tables(port_name, data, years, months, port_out_dir)
        write_yearly_summary(port_name, data, years, port_out_dir)
        print(f"Processed {port_name}: years={years}, months={months}, out={port_out_dir}")


if __name__ == '__main__':
    process_all_ports()
