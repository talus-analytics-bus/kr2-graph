import ncbi

# get ID from text name
def id_search(name):
    params = {"db": "Taxonomy", "term": name}

    soup = ncbi.api_soup("esearch", params)
    ncbi_id = soup.find("Id").getText()

    return ncbi_id
