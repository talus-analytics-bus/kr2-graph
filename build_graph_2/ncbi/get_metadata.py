import ncbi
from bs4 import Tag


# get ncbi metadata from id
def get_metadata(ncbi_id):
    params = {"db": "Taxonomy", "id": ncbi_id}
    soup = ncbi.api_soup("efetch", params)

    taxon = soup.TaxaSet.Taxon

    # print(soup.prettify())

    taxon_metadata = {
        "ScientificName": taxon.ScientificName.getText(),
        "ParentTaxId": taxon.ParentTaxId.getText(),
        "Rank": taxon.Rank.getText(),
        "Division": taxon.Division.getText(),
        "GeneticCode": {"GCId": taxon.GCId.getText(), "GCName": taxon.GCName.getText()},
        "MitoGeneticCode": {
            "MGCId": taxon.MGCId.getText(),
            "MGCName": taxon.MGCName.getText(),
        },
        "Lineage": taxon.Lineage.getText(),
        "CreateDate": taxon.CreateDate.getText(),
        "UpdateDate": taxon.UpdateDate.getText(),
        "PubDate": taxon.PubDate.getText(),
        # "LineageEx":taxon.LineageEx.getText(),
    }

    if taxon.otherNames:
        taxon["OtherNames"] = (taxon.OtherNames.getText(),)

    lineage_ex = []
    for taxon in taxon.LineageEx.children:
        if isinstance(taxon, Tag):
            lineage_ex.append(
                {
                    "TaxId": taxon.TaxId.getText(),
                    "ScientificName": taxon.ScientificName.getText(),
                    "Rank": taxon.Rank.getText(),
                }
            )

    taxon_metadata["LineageEx"] = lineage_ex

    # print()

    return taxon_metadata
