import neo4j
from neo4j.exceptions import ClientError
from typing import Any, Optional
from utils import chat

NODE_PROPERTIES_QUERY = """
CALL db.schema.nodeTypeProperties()
YIELD nodeType, nodeLabels, propertyName, propertyTypes
WITH nodeLabels[0] AS label, collect({property: propertyName, type: propertyTypes[0]}) AS properties
RETURN {labels: label, properties: properties} AS output
"""

REL_PROPERTIES_QUERY = """
CALL db.schema.relTypeProperties()
YIELD relType, propertyName, propertyTypes
WITH relType, collect({property: propertyName, type: propertyTypes[0]}) AS properties
RETURN {type: relType, properties: properties} AS output
"""

REL_QUERY = """
CALL db.schema.visualization()
YIELD nodes, relationships
UNWIND relationships AS rel
WITH rel, [node IN nodes WHERE id(node) = id(startNode(rel))][0] AS startNode,
     [node IN nodes WHERE id(node) = id(endNode(rel))][0] AS endNode
WITH labels(startNode)[0] AS startLabel, type(rel) AS relType, labels(endNode)[0] AS endLabel
RETURN DISTINCT {start: startLabel, type: relType, end: endLabel} AS output
"""


def query_database(
    driver: neo4j.Driver, query: str, params: dict[str, Any] = None
) -> list[dict[str, Any]]:
    if params is None:
        params = {}
    data = driver.execute_query(query, params)
    return [r.data() for r in data.records]


def get_schema(
    driver: neo4j.Driver,
) -> str:
    structured_schema = get_structured_schema(driver)

    def _format_props(props: list[dict[str, Any]]) -> str:
        return ", ".join([f"{prop['property']}: {prop['type']}" for prop in props])

    formatted_node_props = [
        f"{label} {{{_format_props(props)}}}"
        for label, props in structured_schema["node_props"].items()
    ]

    formatted_rel_props = [
        f"{rel_type} {{{_format_props(props)}}}"
        for rel_type, props in structured_schema["rel_props"].items()
    ]

    formatted_rels = [
        f"(:{element['start']})-[:{element['type']}]->(:{element['end']})"
        for element in structured_schema["relationships"]
    ]

    return "\n".join(
        [
            "Node properties:",
            "\n".join(formatted_node_props),
            "Relationship properties:",
            "\n".join(formatted_rel_props),
            "The relationships:",
            "\n".join(formatted_rels),
        ]
    )


def get_structured_schema(driver: neo4j.Driver) -> dict[str, Any]:
    node_labels_response = driver.execute_query(NODE_PROPERTIES_QUERY)
    node_properties = [
        data["output"] for data in [r.data() for r in node_labels_response.records]
    ]

    rel_properties_query_response = driver.execute_query(REL_PROPERTIES_QUERY)
    rel_properties = [
        data["output"]
        for data in [r.data() for r in rel_properties_query_response.records]
    ]

    rel_query_response = driver.execute_query(REL_QUERY)
    relationships = [
        data["output"] for data in [r.data() for r in rel_query_response.records]
    ]

    return {
        "node_props": {el["labels"]: el["properties"] for el in node_properties},
        "rel_props": {el["type"]: el["properties"] for el in rel_properties},
        "relationships": relationships,
    }
