import ncbi
from loguru import logger


def id_search(name):
    """Get ID from text search, using NCBI esearch eutil"""

    logger.info(f"Searching NCBI for term {name}")

    params = {"db": "Taxonomy", "term": name}

    soup = ncbi.api_soup("esearch", params)

    try:
        ncbi_id = soup.find("Id").getText()

    except AttributeError:
        errors = soup.find("ErrorList")
        warnings = soup.find("WarningList")

        for error in errors.children:
            logger.error(f"{error.name}: {error.getText()}")

        for warning in warnings.children:
            logger.warning(f"{warning.name}: {warning.getText()}")

        return None

    return ncbi_id
