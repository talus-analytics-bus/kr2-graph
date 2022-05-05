import os
import time

# from pprint import pprint
from dotenv import load_dotenv
from neo4j import GraphDatabase

import dons
import ncbi

load_dotenv()
# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()


def merge_dons_ncbi():
    keys = dons.get_unique_diseases()

    for key in keys:
        Disease1, Disease2 = key.split("|")
        ncbi_id = ncbi.id_search(f"{Disease1} {Disease2}")

        if ncbi_id:
            ncbi_metadata = ncbi.get_metadata(ncbi_id)
            taxon = {**ncbi_metadata, "TaxId": ncbi_id}

            # add taxon and lineage to database
            ncbi.merge_taxon(taxon, SESSION)

        # else:
        #     ## save broken search terms to file
        #     with open("not_found.txt", "a") as f:
        #         f.write(f"{Disease1}, {Disease2}")
        #         f.write("\n")
        #         f.close()

        # resepect api rate limit
        time.sleep(0.4)


if __name__ == "__main__":
    pass

    # ncbi_id = ncbi.id_search("Salmonella enterica")
    # ncbi_metadata = ncbi.get_metadata(ncbi_id)


# print(metadata)

NEO4J_DRIVER.close()
