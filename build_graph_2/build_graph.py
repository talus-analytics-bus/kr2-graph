import re
import os
import csv
import json
import pprint
import boto3
import psycopg2
import itertools
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

subgraph = SESSION.run("MATCH (n) RETURN n LIMIT 10")
print(subgraph)

for node in iter(subgraph):
    print(node.data())


NEO4J_DRIVER.close()
