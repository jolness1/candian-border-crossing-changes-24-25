import csv
import os
from collections import defaultdict
from typing import Dict, List


INPUT_DIR = os.path.join("output", "montana-history")
OUT_ROOT = os.path.join("output", "large-ports")
YEARS = list(range(1996, 2026))  # inclusive 1996..2025


def read_port_history(port: str) -> Dict[str, Dict[int, int]]:
    """Return totals[measure][year] = sum of monthly counts for that measure."""
    path = os.path.join(INPUT_DIR, f"{port}.csv")
    totals = defaultdict(lambda: defaultdict(int))
    if not os.path.isfile(path):
        return totals
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            try:
                y = int(r["year"]) 
                measure = (r.get("crossingType") or "").strip()
                v = int(float(r.get("numberOfCrossings", 0)))
            except Exception:
                continue
            totals[measure][y] += v
    return totals


def write_rows(out_path: str, port: str, rows: List[Dict]):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["port", "year", "count", "changeYoY", "pctChangeYoY"])
        for r in rows:
            writer.writerow([r["port"], r["year"], r["count"], r["change"], r["pct"]])


def format_val(v):
    return "" if v is None else str(v)


def aggregate_port(port: str):
    totals = read_port_history(port)

    # detection rules (conservative):
    is_person = lambda k: ("passeng" in k) or ("pedestrian" in k)
    is_train = lambda k: k == "trains"
    is_vehicle_measure = lambda k: k in ("buses", "personal vehicles", "trucks")

    # build year -> count maps for each category
    people_map = {}
    train_map = {}
    vehicle_map = {}
    container_map = {}
    empty_container_map = {}

    # collect raw measures keys lowercased mapping
    for y in YEARS:
        people_map[y] = None
        train_map[y] = None
        vehicle_map[y] = None
        container_map[y] = None
        empty_container_map[y] = None

    # compute sums
    for measure, years in totals.items():
        key = measure.lower().strip()
        # people
        if is_person(key):
            for y, m in years.items():
                people_map[y] = (people_map.get(y) or 0) + m
        # trains
        if is_train(key):
            for y, m in years.items():
                train_map[y] = (train_map.get(y) or 0) + m
        # vehicles (explicit measures only)
        if is_vehicle_measure(key):
            for y, m in years.items():
                vehicle_map[y] = (vehicle_map.get(y) or 0) + m
        # containers (any measure containing 'container')
        if 'container' in key:
            for y, m in years.items():
                container_map[y] = (container_map.get(y) or 0) + m
        # empty containers (measures containing 'empty')
        if 'empty' in key:
            for y, m in years.items():
                empty_container_map[y] = (empty_container_map.get(y) or 0) + m

    # prepare rows for CSVs
    people_rows = []
    train_rows = []
    vehicle_rows = []
    container_rows = []
    empty_container_rows = []
    for y in YEARS:
        # people
        curr = people_map.get(y)
        prev = people_map.get(y - 1)
        change = None
        pct = None
        if curr is not None and prev is not None:
            change = curr - prev
            if prev != 0:
                pct = round((change / prev) * 100)
        people_rows.append({"port": port, "year": y, "count": format_val(curr), "change": format_val(change), "pct": format_val(pct)})

        # trains
        curr = train_map.get(y)
        prev = train_map.get(y - 1)
        change = None
        pct = None
        if curr is not None and prev is not None:
            change = curr - prev
            if prev != 0:
                pct = round((change / prev) * 100)
        train_rows.append({"port": port, "year": y, "count": format_val(curr), "change": format_val(change), "pct": format_val(pct)})

        # vehicles
        curr = vehicle_map.get(y)
        prev = vehicle_map.get(y - 1)
        change = None
        pct = None
        if curr is not None and prev is not None:
            change = curr - prev
            if prev != 0:
                pct = round((change / prev) * 100)
        vehicle_rows.append({"port": port, "year": y, "count": format_val(curr), "change": format_val(change), "pct": format_val(pct)})

        # containers
        curr = container_map.get(y)
        prev = container_map.get(y - 1)
        change = None
        pct = None
        if curr is not None and prev is not None:
            change = curr - prev
            if prev != 0:
                pct = round((change / prev) * 100)
        container_rows.append({"port": port, "year": y, "count": format_val(curr), "change": format_val(change), "pct": format_val(pct)})

        # empty containers
        curr = empty_container_map.get(y)
        prev = empty_container_map.get(y - 1)
        change = None
        pct = None
        if curr is not None and prev is not None:
            change = curr - prev
            if prev != 0:
                pct = round((change / prev) * 100)
        empty_container_rows.append({"port": port, "year": y, "count": format_val(curr), "change": format_val(change), "pct": format_val(pct)})

    out_dir = os.path.join(OUT_ROOT, port)
    os.makedirs(out_dir, exist_ok=True)
    write_rows(os.path.join(out_dir, "absolute-people-totals.csv"), port, people_rows)
    write_rows(os.path.join(out_dir, "absolute-train-totals.csv"), port, train_rows)
    write_rows(os.path.join(out_dir, "absolute-vehicle-totals.csv"), port, vehicle_rows)
    write_rows(os.path.join(out_dir, "absolute-container-totals.csv"), port, container_rows)
    write_rows(os.path.join(out_dir, "absolute-empty-containers.csv"), port, empty_container_rows)

    return people_rows, train_rows, vehicle_rows, container_rows, empty_container_rows


def main():
    all_people = []
    all_trains = []
    all_vehicles = []
    all_containers = []
    all_empty_containers = []
    # Process every CSV in the input directory (no filtering)
    if not os.path.isdir(INPUT_DIR):
        print(f"Input dir not found: {INPUT_DIR}")
        return
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    # prepare combined maps across all ports
    combined_people = {y: None for y in YEARS}
    combined_train = {y: None for y in YEARS}
    combined_vehicle = {y: None for y in YEARS}
    combined_container = {y: None for y in YEARS}
    combined_empty_container = {y: None for y in YEARS}

    # detection rules for combining
    is_person = lambda k: ("passeng" in k) or ("pedestrian" in k)
    is_train = lambda k: k == "trains"
    is_vehicle_measure = lambda k: k in ("buses", "personal vehicles", "trucks")
    for fn in files:
        port = os.path.splitext(fn)[0]
        p_rows, t_rows, v_rows, c_rows, e_rows = aggregate_port(port)
        all_people.extend(p_rows)
        all_trains.extend(t_rows)
        all_vehicles.extend(v_rows)
        all_containers.extend(c_rows)
        all_empty_containers.extend(e_rows)
        print(f"Wrote aggregates for {port} -> {os.path.join(OUT_ROOT, port)}")

        # also accumulate per-measure year totals for combined "All Ports"
        totals = read_port_history(port)
        for measure, years in totals.items():
            key = (measure or "").lower().strip()
            if is_person(key):
                for y, m in years.items():
                    combined_people[y] = (combined_people.get(y) or 0) + m
            if is_train(key):
                for y, m in years.items():
                    combined_train[y] = (combined_train.get(y) or 0) + m
            if is_vehicle_measure(key):
                for y, m in years.items():
                    combined_vehicle[y] = (combined_vehicle.get(y) or 0) + m
            if 'container' in key:
                for y, m in years.items():
                    combined_container[y] = (combined_container.get(y) or 0) + m
            if 'empty' in key:
                for y, m in years.items():
                    combined_empty_container[y] = (combined_empty_container.get(y) or 0) + m

    # write aggregated combined files at root of OUT_ROOT
    os.makedirs(OUT_ROOT, exist_ok=True)
    write_rows(os.path.join(OUT_ROOT, "absolute-people-totals.csv"), "", all_people)
    write_rows(os.path.join(OUT_ROOT, "absolute-train-totals.csv"), "", all_trains)
    write_rows(os.path.join(OUT_ROOT, "absolute-vehicle-totals.csv"), "", all_vehicles)
    write_rows(os.path.join(OUT_ROOT, "absolute-container-totals.csv"), "", all_containers)
    write_rows(os.path.join(OUT_ROOT, "absolute-empty-containers.csv"), "", all_empty_containers)

    # write combined totals (sum across ports) to OUT_ROOT/all-ports/
    all_out = os.path.join(OUT_ROOT, "all-ports")
    os.makedirs(all_out, exist_ok=True)

    def build_combined_rows(combined_map):
        rows = []
        for y in YEARS:
            curr = combined_map.get(y)
            prev = combined_map.get(y - 1)
            change = None
            pct = None
            if curr is not None and prev is not None:
                change = curr - prev
                if prev != 0:
                    pct = round((change / prev) * 100)
            rows.append({"port": "All Ports", "year": y, "count": format_val(curr), "change": format_val(change), "pct": format_val(pct)})
        return rows

    write_rows(os.path.join(all_out, "absolute-people-totals.csv"), "All Ports", build_combined_rows(combined_people))
    write_rows(os.path.join(all_out, "absolute-train-totals.csv"), "All Ports", build_combined_rows(combined_train))
    write_rows(os.path.join(all_out, "absolute-vehicle-totals.csv"), "All Ports", build_combined_rows(combined_vehicle))
    write_rows(os.path.join(all_out, "absolute-container-totals.csv"), "All Ports", build_combined_rows(combined_container))
    write_rows(os.path.join(all_out, "absolute-empty-containers.csv"), "All Ports", build_combined_rows(combined_empty_container))

    print(f"Wrote aggregated files to {OUT_ROOT} and combined totals to {all_out}")


if __name__ == '__main__':
    main()
