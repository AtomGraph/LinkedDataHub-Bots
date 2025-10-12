"""Extract entities and reconcile them with DBpedia/Wikidata in a single LLM call."""

import os
import json
from typing import Dict, Any, Union, ClassVar
from rdflib import Graph, Literal, Namespace, BNode, URIRef
from rdflib.namespace import RDF, OWL
from web_algebra.operation import Operation
from web_algebra.mcp_tool import MCPTool
from mcp import types
from openai import OpenAI as OpenAIClient


# Define Namespaces
SCHEMA = Namespace("http://schema.org/")
WD = Namespace("http://www.wikidata.org/entity/")


class ExtractAndReconcileOpenAI(Operation, MCPTool):
    """
    Extracts named entities from text AND reconciles them to DBpedia/Wikidata URIs
    in a single LLM call for improved context awareness and efficiency.
    """

    # System prompt with instructions
    SYSTEM_PROMPT: ClassVar[str] = """You are an expert at named entity recognition and entity linking to knowledge bases.

Your task is to extract named entities from news article text and link them to DBpedia and Wikidata.

IMPORTANT GUIDELINES:
1. Extract ALL significant entities of the specified types
2. For each entity, identify the correct DBpedia and Wikidata URIs
3. Use the article CONTEXT to disambiguate (e.g., "Apple" in tech context = Apple Inc., not the fruit)
4. DBpedia URIs format: http://dbpedia.org/resource/Entity_Name
5. Wikidata URIs format: http://www.wikidata.org/entity/Q12345
6. If you cannot confidently identify URIs for an entity, use empty strings
7. Be precise - wrong URIs are worse than no URIs

Return a JSON object with an "entities" array in this exact format:
{
  "entities": [
    {
      "name": "Entity Name",
      "type": "Person",
      "dbpedia": "http://dbpedia.org/resource/...",
      "wikidata": "http://www.wikidata.org/entity/Q..."
    },
    {
      "name": "Another Entity",
      "type": "Organization",
      "dbpedia": "http://dbpedia.org/resource/...",
      "wikidata": "http://www.wikidata.org/entity/Q..."
    }
  ]
}

If an entity cannot be confidently reconciled, use empty strings for URIs."""

    # User prompt template with data placeholders
    USER_PROMPT_TEMPLATE: ClassVar[str] = """Entity types to extract: {entity_types}

Article text:
{text}"""

    @classmethod
    def description(cls) -> str:
        return "Extracts named entities from text and immediately reconciles them to DBpedia/Wikidata URIs using OpenAI for context-aware extraction and disambiguation. Returns RDF Graph with entities linked to knowledge bases."

    @classmethod
    def inputSchema(cls) -> dict:
        return {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Text to extract and reconcile entities from"
                },
                "article": {
                    "type": "string",
                    "description": "URI of the article to link entities to via schema:mentions"
                },
                "entity_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Types of entities to extract (e.g., 'Person', 'Organization', 'Place')",
                    "default": ["Person", "Organization", "Place"]
                }
            },
            "required": ["text", "article"]
        }

    def execute(self, text: Literal, article: Union[URIRef, BNode], entity_types: list[str] = None) -> Graph:
        """
        Pure function: Extract and reconcile entities from text in a single step

        :param text: Text to extract entities from
        :param article: Article URI/node to link entities to
        :param entity_types: Types of entities to extract
        :return: New RDF Graph containing extracted entities with owl:sameAs links
        """
        # Get OpenAI API key from settings
        openai_api_key = getattr(self.settings, 'openai_api_key', None) or os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in settings or environment")

        # Use provided entity types or defaults
        if entity_types is None:
            entity_types = ["Person", "Organization", "Place"]

        # Create OpenAI client
        client = OpenAIClient(api_key=openai_api_key)

        # Convert Literal to string
        text_str = str(text)

        # Extract and reconcile entities using OpenAI in single call
        entities = self._extract_and_reconcile(text_str, entity_types, client)

        # Create new graph with entities
        entities_graph = Graph()

        # Bind namespaces
        entities_graph.bind("wd", WD)
        entities_graph.bind("owl", OWL)

        self._add_entities_to_graph(entities_graph, entities, article)

        return entities_graph

    def execute_json(self, arguments: dict, variable_stack: list = []) -> Graph:
        """JSON execution: process arguments and delegate to execute()"""
        # Process text argument
        text = Operation.process_json(
            self.settings, arguments["text"], self.context, variable_stack
        )

        # Process article argument (required)
        article = Operation.process_json(
            self.settings, arguments["article"], self.context, variable_stack
        )

        entity_types = None
        if "entity_types" in arguments:
            entity_types = Operation.process_json(
                self.settings, arguments["entity_types"], self.context, variable_stack
            )

        return self.execute(text, article, entity_types)

    def mcp_run(self, arguments: dict, context: Any = None) -> list[types.TextContent]:
        """MCP execution: plain args → plain results"""
        text = Literal(arguments["text"])
        article = URIRef(arguments["article"])
        entity_types = arguments.get("entity_types")

        result_graph = self.execute(text, article, entity_types)

        # Serialize to Turtle for MCP response
        turtle_str = result_graph.serialize(format="turtle")
        return [types.TextContent(type="text", text=turtle_str)]

    def _extract_and_reconcile(self, text: str, entity_types: list[str], client: OpenAIClient) -> list[dict]:
        """
        Extract entities and reconcile to DBpedia/Wikidata in a single LLM call

        :param text: Text to extract entities from
        :param entity_types: List of entity types to extract
        :param client: OpenAI client instance
        :return: List of entity dicts with names, types, and URIs
        """
        # Format user prompt with data
        entity_types_str = ", ".join(entity_types)
        user_prompt = self.USER_PROMPT_TEMPLATE.format(
            entity_types=entity_types_str,
            text=text[:4000]
        )

        try:
            # Use OpenAI client with JSON mode, system + user messages
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )

            # Parse JSON response (JSON mode guarantees valid JSON)
            response_data = json.loads(response.choices[0].message.content)

            # Handle both array and object with "entities" key
            if isinstance(response_data, list):
                entities_list = response_data
            elif isinstance(response_data, dict) and "entities" in response_data:
                entities_list = response_data["entities"]
            else:
                entities_list = []

            # Validate and clean the response
            cleaned_entities = []
            for entity in entities_list:
                if entity.get("name") and entity.get("type"):
                    cleaned_entities.append({
                        "name": entity["name"],
                        "type": entity["type"],
                        "dbpedia": entity.get("dbpedia", ""),
                        "wikidata": entity.get("wikidata", "")
                    })

            return cleaned_entities

        except json.JSONDecodeError as e:
            # If parsing fails, return empty list
            print(f"Warning: JSON parsing failed: {e}")
            print(f"Response was: {response.message.content[:500]}")
            return []
        except Exception as e:
            print(f"Warning: Entity extraction and reconciliation failed: {e}")
            return []

    def _add_entities_to_graph(self, graph: Graph, entities: list[dict], article: Union[URIRef, BNode]) -> None:
        """
        Add extracted and reconciled entities to the RDF graph

        :param graph: RDF Graph to add entities to
        :param entities: List of entity dicts with names, types, and URIs
        :param article: Article URI or BNode to link entities to
        """
        for entity_data in entities:
            entity_name = entity_data["name"]
            entity_type = entity_data["type"]
            dbpedia_uri = entity_data.get("dbpedia", "")
            wikidata_uri = entity_data.get("wikidata", "")

            # Create entity node
            entity_node = graph.resource(BNode())

            # Set entity type based on schema.org vocabulary
            match entity_type:
                case "Person":
                    schema_type = SCHEMA.Person
                case "Organization":
                    schema_type = SCHEMA.Organization
                case "Place":
                    schema_type = SCHEMA.Place
                case _:
                    # Generic Thing type for other entities
                    schema_type = SCHEMA.Thing

            entity_node.set(RDF.type, schema_type)
            entity_node.set(SCHEMA.name, Literal(entity_name))

            # Add owl:sameAs links if URIs were provided
            if dbpedia_uri:
                graph.add((entity_node._identifier, OWL.sameAs, URIRef(dbpedia_uri)))

            if wikidata_uri:
                graph.add((entity_node._identifier, OWL.sameAs, URIRef(wikidata_uri)))

            # Link entity to article using schema:mentions
            graph.add((article, SCHEMA.mentions, entity_node._identifier))
