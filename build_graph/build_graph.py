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

# pull env vars for auth and create neo4j driver
NEO4J_AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)


def get_db_cursor():
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="us-west-1")
    response = client.get_secret_value(SecretId="talus-prod-1")["SecretString"]
    secret = json.loads(response)

    conn = psycopg2.connect(
        database="metric-kr2",
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port="5432",
    )

    cursor = conn.cursor()

    return (conn, cursor)


DB_CONN, DB_CURSOR = get_db_cursor()


def add_diseases():
    DB_CURSOR.execute("SELECT DISTINCT name FROM disease")
    diseases = DB_CURSOR.fetchall()

    with NEO4J_DRIVER.session() as session:
        for disease in diseases:
            session.run(f'CREATE (n:Disease {{ name: "{disease[0]}" }})')

    # dump disease list to file
    # diseaselist = [disease[0] for disease in list(diseases)]
    # diseaselist.sort()
    # for disease in diseaselist:
    #     print(disease)


def add_disease_types():
    DB_CURSOR.execute("select distinct disease_type from disease d")
    disease_types = DB_CURSOR.fetchall()

    with NEO4J_DRIVER.session() as session:
        for disease_type in disease_types:
            session.run(f'CREATE (n:SyndromicCategory {{ name: "{disease_type[0]}" }})')


def link_diseases_families():
    DB_CURSOR.execute("select name, disease_type from disease")
    links = DB_CURSOR.fetchall()

    with NEO4J_DRIVER.session() as session:
        for link in links:
            session.run(
                "MATCH (a:Disease), (b:SyndromicCategory) "
                f'WHERE a.name = "{link[0]}" AND b.name = "{link[1]}" '
                "CREATE (a)<-[r:CONTAINS]-(b) "
            )


def read_symptoms_csv():
    rows = []
    with open("data/WIP_Symptoms and diseases.csv") as symptoms_file:
        for row in csv.reader(symptoms_file):
            _, disease, symptom, freq = row
            if disease == "Disease":
                continue

            rows.append([disease.strip(), symptom.strip(), freq])

    return rows


def add_symptoms():
    # DB_CURSOR.execute("select distinct name from symptom")
    # symptoms = DB_CURSOR.fetchall()

    symptoms_rows = read_symptoms_csv()
    symptoms = set([row[1] for row in symptoms_rows])

    with NEO4J_DRIVER.session() as session:
        for symptom in symptoms:
            # print(f'CREATE (n:Symptom {{ name: "{symptom}" }})')
            session.run(f'CREATE (n:Symptom {{ name: "{symptom}" }})')


def link_symptoms():
    symptoms = read_symptoms_csv()

    with NEO4J_DRIVER.session() as session:
        for link in symptoms:
            session.run(
                "MATCH (a:Disease), (b:Symptom) "
                f'WHERE a.name = "{link[0]}" AND b.name = "{link[1]}" '
                f'CREATE (a)-[r:CAUSES {{frequency: "{link[2]}"}}]->(b) '
            )


# replace countries list with Geonames
def read_countries_csv():
    rows = []
    with open("data/Lookup Countries-Grid view.csv") as countries_file:
        for row in csv.reader(countries_file):
            iso3, name, region, _ = row

            rows.append([iso3.strip(), name.strip(), region.strip()])

    return rows


# replace relationships with geonames
def add_countries():
    countries = read_countries_csv()

    with NEO4J_DRIVER.session() as session:
        for country in countries:
            session.run(
                f'CREATE (n:Country {{ iso3: "{country[0]}", name: "{country[1]}" }})'
            )


# connect WHO regions to geonames
def add_regions():
    countries = read_countries_csv()
    regions = set([row[2] for row in countries])

    with NEO4J_DRIVER.session() as session:
        for region in regions:
            if region != "":
                session.run(f'CREATE (n:Region {{ name: "{region}" }})')


def link_regions():
    countries = read_countries_csv()

    with NEO4J_DRIVER.session() as session:
        for link in countries:
            session.run(
                "MATCH (a:Region), (b:Country) "
                f'WHERE a.name = "{link[2]}" AND b.iso3 = "{link[0]}" '
                f"CREATE (a)-[r:CONTAINS]->(b) "
            )


def read_dons_csv():
    rows = []
    with open("data/DONdatabase.csv") as dons_file:
        rows = list(csv.DictReader(dons_file))

    return rows


def add_dons():
    rows = read_dons_csv()

    dons = {}
    for row in rows:
        if row["DONid"] in dons.keys():
            dons[row["DONid"]].append(row)
        else:
            dons[row["DONid"]] = [row]

    with NEO4J_DRIVER.session() as session:
        for DONid in dons.keys():
            session.run(
                "CREATE (n:DON { "
                f'DONid: "{DONid}", '
                f'Link: "{dons[DONid][0]["Link"]}", '
                f'Headline: "{dons[DONid][0]["Headline"]}", '
                f'ReportDate: "{dons[DONid][0]["ReportDate"]}" '
                "})"
            )

    ## dump DONs disease list to file
    # diseaselist = list(set([dons[key][0]["DiseaseLevel1"] for key in dons.keys()]))
    # diseaselist.sort()
    # for disease in diseaselist:
    #     print(disease)


def escape_quotes(string):
    return string.replace('"', '\\"')


def link_dons_countries():
    rows = read_dons_csv()

    with NEO4J_DRIVER.session() as session:
        for link in rows:
            iso3 = link["ISO"]
            if link["Country"] == "Global":
                iso3 = "GLB"

            session.run(
                "MATCH (a:DON), (b:Country) "
                f'WHERE a.DONid = "{link["DONid"]}" AND b.iso3 = "{iso3}" '
                "CREATE (a)-[r:OUTBREAK { "
                f'Notes: "{escape_quotes(link["Notes"])} ", '
                f'CasesTotal: "{link["CasesTotal"]}", '
                f'Deaths: "{link["Deaths"]}", '
                f'CasesConfirmed: "{link["CasesConfirmed"]}", '
                f'OutbreakEpicenter: "{escape_quotes(link["OutbreakEpicenter"])}" '
                "}]->(b) "
            )


def disease_link_map():
    dons_name_to_disease_name = {}
    with open("data/disease_inner_matches.csv") as dons_file:
        for row in csv.reader(dons_file):
            disease, dons_disease = row
            dons_name_to_disease_name[dons_disease] = disease

    return dons_name_to_disease_name


def link_dons_diseases():
    rows = read_dons_csv()
    name_map = disease_link_map()

    dons_diseases = set()
    for row in rows:
        dons_diseases.add(row["DONid"] + "|" + row["DiseaseLevel1"])

    with NEO4J_DRIVER.session() as session:
        # for link in enumerate(itertools.islice(dons_diseases, 100)):
        for link in dons_diseases:
            DONid, disease_name = link.split("|")

            if disease_name in name_map.keys():
                session.run(
                    "MATCH (a:DON), (b:Disease) "
                    f'WHERE a.DONid="{DONid}" AND b.name="{name_map[disease_name]}" '
                    "CREATE (a)-[r:MENTIONS]->(b) "
                )


def get_nih_taxonomy():
    ## HTML response is malformed and missing closing tags so
    ## I'll substitute by parsing the corrected HTML from a file
    # url = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi"
    # data = b"mode=Undef&name=&srchmode=1&keep=1&unlock=1&butt=Display&mode
    #   =Undef&old_id=197911&lvl=2&filter="
    # data = b"mode=Undef&name=&srchmode=1&keep=1&unlock=1&butt=Display&mode
    #   =Undef&old_id=11308&lvl=3&filter="
    # req = urllib.request.Request(url=url, data=data)

    titles = ["family", "genus", "species", "serotype"]

    def build_taxonomy(soup, depth):
        children = []

        title = titles[depth]

        limit = 20
        for li in itertools.islice(soup.children, limit):
            if isinstance(li, NavigableString):
                continue

            if isinstance(li, Tag):
                ## html source has lots of "no rank" titles so I'm
                ## forcing it with an array instead
                # title = li.select_one("a").get("title")
                # title = re.sub(" {2,}", " ", title).replace("\n", "")

                name = li.select_one("a").get_text()
                name = re.sub(" {2,}", " ", name).replace("\n", "")

                next_soup = li.select_one("ul")

                if next_soup:
                    children.append(
                        {
                            "level": title,
                            "name": name,
                            "children": build_taxonomy(next_soup, depth + 1),
                        }
                    )
                else:
                    children.append({"level": title, "name": name})

        return children

    taxonomy = {}
    # with urllib.request.urlopen(req) as f:
    with open("./data/taxonomy.html") as f:
        soup = BeautifulSoup(f, features="html.parser")
        taxonomy = build_taxonomy(soup, 0)
        # for debugging
        with open("./taxonomy_dump.json", "w") as f:
            f.write(json.dumps(taxonomy))

    return taxonomy


def add_and_link_taxonomy():
    taxonomy = get_nih_taxonomy()

    def add_node(parent, node, session):
        # create node
        session.run(
            f'CREATE (n:{node["level"].capitalize()} {{ name: "{node["name"]}" }})'
        )

        # link to parent
        if parent:
            parent_level = parent["level"].capitalize()
            node_level = node["level"].capitalize()
            session.run(
                f"MATCH (a:{parent_level}), (b:{node_level}) "
                f'WHERE a.name = "{parent["name"]}" AND b.name = "{node["name"]}" '
                "CREATE (a)-[:CONTAINS]->(b) "
            )

        if node["level"] in ["species", "serotype"]:
            session.run(
                f'MATCH (b:{node["level"].capitalize()} {{name: "{node["name"]}"}}), '
                f'    (d:Disease {{name: "Influenza"}}) '
                "CREATE (b)-[:CAUSES]->(d) "
            )

        # recursively add children
        if "children" in node:
            for child in node["children"]:
                add_node(node, child, session)

    with NEO4J_DRIVER.session() as session:
        add_node(None, taxonomy[0], session)


def link_dons_influenza():
    rows = read_dons_csv()

    def add_serotype_and_link(DONid, DiseaseLevel2, session):
        # make the serotype exist, link it if it's being created
        session.run(f'MERGE (s:Serotype {{name: "{DiseaseLevel2} subtype"}}) ')
        session.run(
            f'MATCH (sp:Species {{name: "Influenza A virus"}}), '
            f'  (s:Serotype {{name: "{DiseaseLevel2} subtype"}}) '
            f"MERGE (sp)-[r:CONTAINS]->(s) "
        )
        session.run(
            f'MATCH (d:DON {{DONid: "{DONid}"}}), '
            f'  (s:Serotype {{name: "{DiseaseLevel2} subtype"}}) '
            f"MERGE (d)-[r:MENTIONS]->(s)"
        )

    dons_diseases = set()
    for row in rows:
        if row["DiseaseLevel1"] == "Influenza A" and row["DiseaseLevel2"] != "NA":
            dons_diseases.add(
                row["DONid"] + "|" + row["DiseaseLevel1"] + "|" + row["DiseaseLevel2"]
            )

    with NEO4J_DRIVER.session() as session:
        for link in dons_diseases:
            DONid, _, DiseaseLevel2 = link.split("|")
            add_serotype_and_link(DONid, DiseaseLevel2, session)


# add_diseases()
# add_disease_types()
# link_diseases_families()
# add_symptoms()
# link_symptoms()
# add_countries()
# add_regions()
# link_regions()
# add_dons()
# link_dons_countries()
# link_dons_diseases()
# get_nih_taxonomy()
# add_and_link_taxonomy()
# link_dons_influenza()

DB_CONN.close()
NEO4J_DRIVER.close()
