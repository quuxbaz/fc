import argparse
import json
import csv
from util import *
import fullcontact
import time

def csv_writer(fname, fields):
    """Takes a file name FNAME, a list of fields FIELDS and produces a csv
WRITER object with which we can write rows to."""
    with open(fname, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = fields)
    return writer

def main():
    global ARGS
    parser = argparse.ArgumentParser(description="Reads a CSV and writes a CSV.")
    parser.add_argument("--csv", metavar="INPUT", type=str, required = True,
                        help = "CSV with an e-mail column")
    parser.add_argument("--email-column", metavar="EMAILS", type=str, required = True,
                        help = "column name where the emails are")
    parser.add_argument("--fckey", metavar="KEY", type=str, required = True,
                        help = "fullcontact api key")
    parser.add_argument("--delay", metavar="DELAY", type=float, required = False, default = 1.1)
    parser.add_argument("--row-start", metavar="ROWSTART", type=int, required = False, default = 1)
    ARGS = parser.parse_args()
    setuplog("fc.log")

    # Some fullcontact requests produce HTTP STATUS 202, meaning try
    # again later to see if I can get you anything.  So we add these
    # people to the LATER list as we should try them all at the end of
    # the batch -- though we don't do that at the moment.
    later = []

    # prepares file name for the output
    outfname = ".".join(ARGS.csv.split(".")[0:-1]) + ".out.csv"
    if "." not in ARGS.csv:
        outfname = ARGS.csv + ".out.csv"

    newfields = ["name", "gender", "organizations", "titles", "json"]

    # do the writing
    with open(ARGS.csv, newline = "") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar="\"")
        writer_fields = reader.fieldnames + newfields
        logging.info("Newfields: {}".format(writer_fields))
        outcsv = open(outfname, "a", newline="", encoding='utf-8')
        writer = csv.DictWriter(outcsv, fieldnames = writer_fields)

        if ARGS.row_start == 1:
            # write header only if we're at the beginning
            # otherwise assume the header has already been written
            writer.writeheader()

        email = ARGS.email_column
        
        if ARGS.row_start > 1:
            logging.info("skipping rows 1--{} so I start at row {}".format(ARGS.row_start - 1, ARGS.row_start))

        for i, row in enumerate(reader):
            if i < (ARGS.row_start - 1):
                continue

            row["name"] = ""
            row["gender"] = ""
            row["organizations"] = ""
            row["titles"] = ""
            row["json"] = ""

            logging.info("fetching data on {}".format(row[email]))
            data, headers, status = fullcontact.whois(row[email],{"Authorization": "Bearer {}".format(ARGS.fckey)})
            time.sleep(ARGS.delay)
            if status == 200:
                logging.info("fullcontact: person: {}".format(json.dumps(data)))
                # append to row all the interesting columns.
                # loop through all the new desired columns, adding each to ROW
                row["name"] = json_deep_get(data, ["contactInfo", "fullName"])
                row["gender"] = get_gender(data)
                row["organizations"] = get_organization_item_collect(data, "name")
                row["titles"] = get_organization_item_collect(data, "title")
                row["json"] = json.dumps(data)
            if status == 202:
                later.append(row[email])

            writer.writerow(row)

        for email in later:
            print("These produced 202 (later) HTTP status:")
            print("We should try these again, though we did not.")
            print(email)

if __name__ == "__main__":
    main()
