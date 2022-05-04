import csv


def get_rows():
    rows = []
    with open("../build_graph/data/DONdatabase.csv") as dons_file:
        rows = list(csv.DictReader(dons_file))

    return rows
