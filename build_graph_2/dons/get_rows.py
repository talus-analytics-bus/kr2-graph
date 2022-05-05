import csv


def get_rows():
    rows = []
    with open("dons/data/DONdatabase.csv") as dons_file:
        rows = list(csv.DictReader(dons_file))

    return rows
