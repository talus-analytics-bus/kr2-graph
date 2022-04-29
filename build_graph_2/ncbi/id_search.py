import ncbi
import logging

# get ID from text name
def id_search(name):
    params = {"db": "Taxonomy", "term": name}

    soup = ncbi.api_soup("esearch", params)

    try:
        ncbi_id = soup.find("Id").getText()

    except AttributeError:
        print(soup.prettify())
        errors = soup.find("ErrorList")
        warnings = soup.find("WarningList")

        for error in errors.children:
            logging.error(f'ncbi.id_search({name}): {error.name}: {error.getText()}')

        for warning in warnings.children:
            logging.warning(f'ncbi.id_search({name}): {warning.name}: {warning.getText()}')

        return None

    return ncbi_id


