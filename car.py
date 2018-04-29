from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:11002", auth=("neo4j", "123456"))
session = driver.session()

def print_friends_of(name):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (i:Incident)-[:ENDED]->(f)"
                                "WHERE i.id = {name}"
                                 "RETURN DISTINCT f.name", name=name):
                print(record["f.name"])

def incidents_per_day():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (n:Incident)"
                                "RETURN n.dayofweek as DAY,COUNT(n.dayofweek) as POCET"
                                " ORDER BY POCET DESC",""):
                print(record[0]," "*(9-len(record[0])),":",record[1])

def incidents_in_day_of(day):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (n:Incident)"
                                "WHERE n.dayofweek = {day}"
                                "RETURN COUNT(n.dayofweek) as P",day=day):
                print(record[0])

def the_most_dangerous_district():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (incident)-[:IN]->(district)"
                                "RETURN district.name, count(incident) as POCET"
                                " ORDER BY POCET DESC"
                                " LIMIT 1",""):
                print(record[0])

def the_most_common_incident():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (incident)-[:WITH]->(category)"
                                "RETURN category.name, count(incident) as POCET"
                                " ORDER BY POCET DESC"
                                " LIMIT 1",""):
                print(record[0])

def the_least_robberies_district():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (incident)-[:WITH]->(robbery), (incident)-[:IN]->(district)"
                                "WHERE robbery.name='ROBBERY' "
                                "RETURN district.name, count(robbery)"
                                "ORDER BY count(robbery) ASC"
                                " LIMIT 1;",""):
                print(record[0])

print_friends_of(150098458)
incidents_per_day()
incidents_in_day_of("Friday")
the_most_dangerous_district()
the_most_common_incident()
the_least_robberies_district()






