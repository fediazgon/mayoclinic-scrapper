#!/usr/bin/env python3

import argparse
import json
import os

from neo4j.v1 import GraphDatabase

query = """
WITH { json_data } AS diseases
UNWIND diseases AS d

MERGE (disease:Disease { id: d.disease_id, name: d.disease_name }) 
FOREACH (c IN d.causes | MERGE (cause:Cause { id: c.cause_id, name: c.cause_name })
  MERGE (disease)-[:CAUSED_BY]->(cause))
FOREACH (r IN d.risk_factors | MERGE (risk:RiskFactor { id: r.risk_id, name: r.risk_name })
  MERGE (disease)-[:HAS_RISK]->(risk))
"""


def file_exists(p, arg):
    if not os.path.exists(arg):
        p.error('File {} does not exists'.format(arg))
    else:
        return arg


def add_diseases(tx, json_data):
    for record in tx.run(query, json_data=json_data):
        print(record)


parser = argparse.ArgumentParser()
parser.add_argument("file", help="MayoClinic diseases json", type=lambda x: file_exists(parser, x))
args = parser.parse_args()

driver = GraphDatabase.driver("bolt://localhost:7687")

with open(args.file) as diseases_file:
    diseases_json = json.load(diseases_file)

with driver.session() as session:
    session.write_transaction(add_diseases, diseases_json)
