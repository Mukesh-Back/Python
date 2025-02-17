from neo4j import GraphDatabase

class Neo4jProcedure:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_custom_procedure(self, param1, param2):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_and_return_procedure, param1, param2)
            for record in result:
                print(record)

    @staticmethod
    def _create_and_return_procedure(tx, param1, param2):
        query = (
            "CALL apoc.create.node(['CustomNode'], {param1: $param1, param2: $param2}) "
            "YIELD node "
            "RETURN node"
        )
        result = tx.run(query, param1=param1, param2=param2)
        return [record["node"] for record in result]

# Usage
if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "your_password"

    neo4j_procedure = Neo4jProcedure(uri, user, password)
    neo4j_procedure.create_custom_procedure("value1", "value2")
    neo4j_procedure.close()
