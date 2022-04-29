import os
import csv
import logging
from dotenv import load_dotenv
from neo4j import GraphDatabase
import time

import ncbi

load_dotenv()
## pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

# subgraph = SESSION.run("MATCH (n) RETURN n LIMIT 10")

logging.basicConfig(filename='build_graph_debug.log', encoding='utf-8', level=logging.DEBUG)


def read_dons_csv():
    rows = []
    with open("../build_graph/data/DONdatabase.csv") as dons_file:
        rows = list(csv.DictReader(dons_file))

    return rows


def get_dons_disease_set(rows):
    dons_diseases = set()
    for row in rows:
        if row["DiseaseLevel1"] == "Influenza A" and row["DiseaseLevel2"] != "NA":
            dons_diseases.add(
                row["DiseaseLevel1"] + "|" + row["DiseaseLevel2"]
            )

    return dons_diseases


# merge ncbi lineage into graph
def merge_lineage(metadata):
    pass


rows = read_dons_csv()
keys = get_dons_disease_set(rows)

for key in keys:
    Disease1, Disease2 = key.split("|")
    print("\n", Disease2.strip())

    ncbi_id = ncbi.id_search(Disease2)

    if not ncbi_id:
        ## save broken search terms to file
        with open('not_found.txt', 'a') as f:
            f.write(f'{Disease1}, {Disease2}')
            f.write('\n')
            f.close()

    time.sleep(.4)

# print(ncbi.id_search('H5N6'))

# print(get_dons_disease_set(read_dons_csv()))

# ncbi_id = ncbi.id_search("h1n1")
# metadata = ncbi.get_metadata(ncbi_id)

# print(metadata)

NEO4J_DRIVER.close()
