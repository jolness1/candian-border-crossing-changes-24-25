import csv
import glob
import os
from collections import defaultdict
from datetime import datetime


def find_input_file(input_dir="input-data"):
	pattern = os.path.join(input_dir, "Border_Crossing*csv")
	matches = glob.glob(pattern)
	return matches[0] if matches else None


def sanitize_filename(name: str) -> str:
	# strip and replace problematic characters
	s = name.strip()
	for ch in ("/", "\\", ":", "*", "?", '"', "<", ">", "|"):
		s = s.replace(ch, "_")
	return s


def parse_and_write(input_path, output_dir="output/montana-history"):
	os.makedirs(output_dir, exist_ok=True)

	per_port = defaultdict(lambda: defaultdict(int))
	# per_port[port][(year, month_num, month_abbr, measure)] = total

	with open(input_path, newline="", encoding="utf-8") as fh:
		reader = csv.DictReader(fh)
		for row in reader:
			try:
				if row.get("State", "").strip() != "Montana":
					continue
				if row.get("Border", "").strip() != "US-Canada Border":
					continue

				port = row.get("Port Name", "").strip()
				date_str = row.get("Date", "").strip()
				measure = row.get("Measure", "").strip()
				value_raw = row.get("Value", "").strip()

				# parse date (Format is 'Jan 2024')
				try:
					dt = datetime.strptime(date_str, "%b %Y")
				except Exception:
					# skip rows with bad dates
					continue

				year = dt.year
				month_num = dt.month
				month_abbr = dt.strftime("%b")

				# convert value to int where possible
				try:
					if value_raw == "":
						value = 0
					else:
						value = int(float(value_raw))
				except Exception:
					# fallback to 0 if unparsable
					value = 0

				key = (year, month_num, month_abbr, measure)
				per_port[port][key] += value
			except Exception:
				# ignore problematic rows but continue
				continue

	# one CSV per port
	for port, agg in per_port.items():
		safe_name = sanitize_filename(port)
		out_path = os.path.join(output_dir, f"{safe_name}.csv")
		# sort chronologically
		keys = sorted(agg.keys(), key=lambda k: (k[0], k[1]))
		with open(out_path, "w", newline="", encoding="utf-8") as ofh:
			writer = csv.writer(ofh)
			writer.writerow(["year", "month", "crossingType", "numberOfCrossings"])
			for (year, _month_num, month_abbr, measure) in keys:
				total = agg[(year, _month_num, month_abbr, measure)]
				writer.writerow([year, month_abbr, measure, total])


def main():
	input_file = find_input_file("input-data")
	if not input_file:
		print("Could not find Border_Crossing CSV in ./input-data. Exiting.")
		return

	print(f"Using input: {input_file}")
	outdir = "output/montana-history"
	parse_and_write(input_file, output_dir=outdir)
	print(f"Wrote per-port CSVs to ./{outdir}/")


if __name__ == "__main__":
	main()
