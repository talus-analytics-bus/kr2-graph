import dons


def get_unique_diseases():
    rows = dons.get_rows()

    dons_diseases = set()
    for row in rows:
        # if row["DiseaseLevel1"] == "Influenza A" and row["DiseaseLevel2"] != "NA":
        if row["DiseaseLevel2"] != "NA":
            dons_diseases.add(row["DiseaseLevel1"] + "|" + row["DiseaseLevel2"])
        else:
            dons_diseases.add(row["DiseaseLevel1"] + "|")

    return dons_diseases
