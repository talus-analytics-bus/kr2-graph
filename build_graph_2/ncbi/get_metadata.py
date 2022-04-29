import ncbi

# get ncbi metadata from id
def get_metadata(ncbi_id):
    params = {"db": "Taxonomy", "id": ncbi_id}
    soup = ncbi.api_soup("efetch", params)

    taxon = soup.TaxaSet.Taxon

    taxon_metadata = {
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

    return taxon_metadata
