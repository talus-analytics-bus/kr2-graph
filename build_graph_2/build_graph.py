import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase

import ncbi

load_dotenv()
## pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

# subgraph = SESSION.run("MATCH (n) RETURN n LIMIT 10")


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
                row["DONid"] + "|" + row["DiseaseLevel1"] + "|" + row["DiseaseLevel2"]
            )

    return dons_diseases


# merge ncbi lineage into graph
def merge_lineage(metadata):
    pass


# print(subgraph)

# for node in iter(subgraph):
#     print(node.data())

# rows = read_dons_csv()
# print(get_dons_disease_set(rows))
ncbi_id = ncbi.id_search("h1n1")

metadata = ncbi.get_metadata(ncbi_id)
print(metadata)

NEO4J_DRIVER.close()
