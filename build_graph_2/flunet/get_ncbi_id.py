import csv
from functools import cache

import ncbi
import flunet


@cache
def column_to_ncbi_name():
    agent_map = {}
    with open("flunet/data/flunet_to_ncbi.csv") as map:
        for row in csv.reader(map):
            flunet, ncbi = row
            agent_map[flunet] = ncbi

    if flunet in agent_map:
        return agent_map[flunet]

    return None


@cache
def get_ncbi_id(key):
    ncbi_term = flunet.column_to_ncbi_name(key)

    if ncbi_term:
        return ncbi.id_search(ncbi_term)

    return None
