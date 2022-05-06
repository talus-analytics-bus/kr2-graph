import os
import time

# from pprint import pprint
from loguru import logger
from functools import cache
from dotenv import load_dotenv
from neo4j import GraphDatabase


import dons
import ncbi
import flunet

load_dotenv()
# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()


def db_merge_dons_ncbi():
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


@cache
def flunet_to_ncbi_id(key):
    ncbi_term = flunet.column_to_ncbi(key)

    if ncbi_term:
        return ncbi.id_search(ncbi_term)

    return None


if __name__ == "__main__":
    flunet_rows = flunet.get_rows()

    merged_countries = set()
    merged_transmission_zones = set()
    merged_taxons = set()

    detection_cols = {}
    for key in flunet_rows[0].keys():
        ncbi_id = flunet_to_ncbi_id(key)
        if ncbi_id:
            detection_cols[key] = ncbi_id

    for index, row in enumerate(flunet_rows):
        # skip rows where none were collected
        if row["Collected"] == "" or row["Collected"] == "0":
            continue

        if row["Transmission zone"] not in merged_transmission_zones:
            logger.info(f' MERGE transmission zone node ({row["Transmission zone"]})')
            SESSION.run(
                f'MERGE (n:TransmissionZone:Geo {{name: "{row["Transmission zone"]}"}})'
            )
            logger.info(
                f"Linking Territory {row['Territory']} to {row['Transmission zone']}"
            )
            merged_transmission_zones.add(row["Transmission zone"])

        if row["Territory"] not in merged_countries:
            logger.info(f' MERGE country node ({row["Territory"]})')
            SESSION.run(f'MERGE (n:Country:Geo {{name: "{row["Territory"]}"}})')

            # also join the country to the transmission zone
            SESSION.run(
                f'MATCH (country:Country {{name: "{row["Territory"]}"}}), '
                f'  (zone:TransmissionZone {{name: "{row["Transmission zone"]}"}}) '
                f"MERGE (country)-[:IN]->(zone) "
            )

            merged_countries.add(row["Territory"])

        logger.info(f"Merging FluNet Report {index}")
        SESSION.run(
            f"MERGE (n:FluNet:Report {{"
            f"  flunetRow: {index}, "
            f'  start: date("{row["Start date"]}"), '
            f'  end: date("{row["End date"]}"), '
            f'  specimensCollected: {row["Collected"] or 0}, '
            f'  specimensProcessed: {row["Processed"] or 0} '
            f"}})"
        )

        logger.info(f"Linking FluNet Report {index} to {row['Territory']}")
        SESSION.run(
            f"MATCH (report:FluNet {{flunetRow: {index}}}), "
            f'  (country:Country {{name: "{row["Territory"]}"}}) '
            f"MERGE (report)-[:IN]->(country) "
        )

        for col in detection_cols.keys():
            # skip detection columns with no values
            # or with zero specimens detected
            if not row[col] or row[col] == "0":
                continue

            ncbi_id = detection_cols[col]

            if ncbi_id not in merged_taxons:
                ncbi_metadata = ncbi.get_metadata(ncbi_id)
                taxon = {**ncbi_metadata, "TaxId": ncbi_id}
                ncbi.merge_taxon(taxon, SESSION)
                merged_taxons.add(ncbi_id)

            logger.info(f"Linking FluNet Report {index} to Taxon {ncbi_id}")

            SESSION.run(
                f"MATCH (report:FluNet {{flunetRow: {index}}}), "
                f'  (taxon:Taxon {{TaxId: "{ncbi_id}"}}) '
                f"MERGE (report)-[:DETECTED {{count: {row[col]}}}]->(taxon) "
            )

    # ncbi_id = ncbi.id_search("Salmonella enterica")
    # ncbi_metadata = ncbi.get_metadata(ncbi_id)


# print(metadata)

NEO4J_DRIVER.close()
