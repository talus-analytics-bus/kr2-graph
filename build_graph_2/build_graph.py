import re
import os
import csv
import json
import pprint
import boto3
import psycopg2
import itertools
import requests
import lxml
from dotenv import load_dotenv
from neo4j import GraphDatabase
from bs4 import BeautifulSoup, NavigableString, Tag

load_dotenv()
pp = pprint.PrettyPrinter(indent=4)

## pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
SESSION = NEO4J_DRIVER.session()

# subgraph = SESSION.run("MATCH (n) RETURN n LIMIT 10")


def get_ncbi_api(eutil, params):
    response = requests.get(
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{eutil}.fcgi", params=params
    )
    soup = BeautifulSoup(response.content, features="xml")

    return soup


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


# get ID from text name
def get_ncbi_id(name):
    params = {"db": "Taxonomy", "term": name}

    soup = get_ncbi_api("esearch", params)
    ncbi_id = soup.find("Id").getText()

    return ncbi_id


# get ncbi metadata from id
def get_ncbi_metadata(ncbi_id):
    params = {"db": "Taxonomy", "id": ncbi_id}
    soup = get_ncbi_api("efetch", params)

    taxon = soup.TaxaSet.Taxon

    taxon_node = {
        "ScientificName": taxon.ScientificName.getText(),
        "OtherNames": taxon.OtherNames.getText(),
        "ParentTaxId": taxon.ParentTaxId.getText(),
        "Rank": taxon.Rank.getText(),
        "Division": taxon.Division.getText(),
        "GeneticCode": {"GCId": taxon.GCId.getText(), "GCName": taxon.GCName.getText()},
        "MitoGeneticCode": {
            "MGCId": taxon.MGCId.getText(),
            "MGCName": taxon.MGCName.getText(),
        },
        # "Lineage": taxon.Lineage.getText(),
        # "LineageEx":taxon.LineageEx.getText(),
    }

    for taxon in taxon.LineageEx.children:
        print(taxon)

    return taxon_node


# merge ncbi lineage into graph
def merge_lineage(metadata):
    pass


# print(subgraph)

# for node in iter(subgraph):
#     print(node.data())

# rows = read_dons_csv()
# print(get_dons_disease_set(rows))
ncbi_id = get_ncbi_id("h1n1")
metadata = get_ncbi_metadata(ncbi_id)
print(metadata)

NEO4J_DRIVER.close()
