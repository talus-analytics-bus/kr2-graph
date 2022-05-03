import os
import csv
from pprint import pprint
from dotenv import load_dotenv
from neo4j import GraphDatabase
from loguru import logger

import time

import ncbi

load_dotenv()
## pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()


def read_dons_csv():
    rows = []
    with open("../build_graph/data/DONdatabase.csv") as dons_file:
        rows = list(csv.DictReader(dons_file))

    return rows


def get_dons_disease_set(rows):
    dons_diseases = set()
    for row in rows:
        # if row["DiseaseLevel1"] == "Influenza A" and row["DiseaseLevel2"] != "NA":
        if row["DiseaseLevel2"] != "NA":
            dons_diseases.add(row["DiseaseLevel1"] + "|" + row["DiseaseLevel2"])
        else:
            dons_diseases.add(row["DiseaseLevel1"] + "|")

    return dons_diseases


def merge_taxon(taxon):
    pprint(taxon["ScientificName"])
    rank = taxon["Rank"].replace(" ", "_")
    SESSION.run(
        f'MERGE (n:{rank} {{name: "{taxon["ScientificName"]}", '
        f'  Rank: "{rank}", '
        f'  TaxId: "{taxon["TaxId"]}" '
        f"}})"
    )


def merge_taxon_link(parent, child):
    parent_rank = parent["Rank"].replace(" ", "_")
    child_rank = child["Rank"].replace(" ", "_")
    SESSION.run(
        f'MATCH (parent:{parent_rank} {{name: "{parent["ScientificName"]}"}}), '
        f'  (child:{child_rank} {{name: "{child["ScientificName"]}"}}) '
        f"MERGE (parent)-[r:CONTAINS]->(child) "
    )


def merge_lineage(lineage):
    for index, taxon in enumerate(lineage):
        # merge parent node
        merge_taxon(taxon)

        # if there are children
        if index + 1 < len(lineage):
            child = lineage[index + 1]
            # merge child node
            merge_taxon(child)
            # merge relationship
            merge_taxon_link(taxon, child)


def merge_ncbi_metadata_response(ncbi_id, ncbi_metadata):
    merge_taxon({**ncbi_metadata, "TaxId": ncbi_id})

    merge_taxon(ncbi_metadata["LineageEx"][-1])

    merge_taxon_link(
        ncbi_metadata["LineageEx"][-1], {**ncbi_metadata, "TaxId": ncbi_id}
    )

    merge_lineage(ncbi_metadata["LineageEx"])


if __name__ == "__main__":
    rows = read_dons_csv()
    keys = get_dons_disease_set(rows)

    for key in keys:
        Disease1, Disease2 = key.split("|")
        ncbi_id = ncbi.id_search(f"{Disease1} {Disease2}")

        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            merge_ncbi_metadata_response(ncbi_id, ncbi_metadata)

        else:
            ## save broken search terms to file
            with open("not_found.txt", "a") as f:
                f.write(f"{Disease1}, {Disease2}")
                f.write("\n")
                f.close()

        # resepect api rate limit
        time.sleep(0.4)

# ncbi_id = ncbi.id_search("anthrax")
# ncbi_metadata = ncbi.get_metadata(ncbi_id)


# print(get_dons_disease_set(read_dons_csv()))

# ncbi_id = ncbi.id_search("h1n1")
# metadata = ncbi.get_metadata(ncbi_id)

# print(metadata)

NEO4J_DRIVER.close()
