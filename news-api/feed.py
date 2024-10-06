import os
import urllib.request
import json
from typing import Dict, Optional, List, Union
from urllib.parse import urlencode
from rdflib import Graph, Literal, Namespace, BNode, URIRef
from rdflib.resource import Resource
from rdflib.namespace import RDF, XSD
import openai

# Define Namespaces
SCHEMA = Namespace("http://schema.org/")

def fetch_data(api_key: str, query_params: Dict) -> Dict:
    # Check if the API key is available
    if not api_key:
        raise ValueError("API key is not provided.")

    # Add the API key to the query parameters
    query_params['apiKey'] = api_key

    # Encode query parameters
    encoded_params = urlencode(query_params)

    # Construct the full URL with the encoded query parameters
    url = f"https://newsapi.org/v2/everything?{encoded_params}"

    # Fetch and process the JSON data within a response context
    with urllib.request.urlopen(url) as response:
        # Load the JSON response
        return json.loads(response.read().decode())

# Function to fetch and convert JSON to RDF, accepts query parameters dict and api_key
def to_graph(data: Dict) -> Graph:
        # Create RDF Graph
        g = Graph()

        # Loop through articles and create RDFLib resources
        for article_data in data['articles']:
            # Create a blank node for the article
            article = g.resource(BNode())

            # Set article type as schema.org Article
            article.set(RDF.type, SCHEMA.Article)

            # Set properties on the RDFLib Resource using Pythonic key checks
            if "title" in article_data:
                article.set(SCHEMA.headline, Literal(article_data['title']))

            if "description" in article_data:
                article.set(SCHEMA.description, Literal(article_data['description']))

            if "author" in article_data:
                article.set(SCHEMA.author, Literal(article_data['author']))

            if "url" in article_data:
                article.set(SCHEMA.url, URIRef(article_data['url']))

            if "publishedAt" in article_data:
                article.set(SCHEMA.datePublished, Literal(article_data['publishedAt'], datatype=XSD.dateTime))

            # Add source information
            if "source" in article_data:
                source_data = article_data['source']
                if "name" in source_data:
                    source = g.resource(BNode())
                    source.set(RDF.type, SCHEMA.Organization)
                    source.set(SCHEMA.name, Literal(source_data['name']))

                    # Link the source (publisher) to the article
                    article.set(SCHEMA.publisher, source)

            # Add content if available
            if "content" in article_data:
                article.set(SCHEMA.articleBody, Literal(article_data['content']))


        # Return the graph in Turtle format
        return g

def extract_named_entities(openai_api_key: str, description: str) -> Optional[List[Dict[str, Optional[str]]]]:
    """
    Extract named entities from the given description using the ChatGPT API with the function_call mode.
    Returns a list of dictionaries, each containing 'name' and 'uri' for the entities.
    """
    
    openai.api_key = openai_api_key  # Replace with your actual API key
    
    # Use GPT-4 with function_call to return well-structured JSON
    response = openai.ChatCompletion.create(
        model="gpt-4-0613",  # Ensure you're using the latest GPT model that supports function calls
        messages=[
            {
                "role": "system",
                "content": "You are an assistant that extracts named entities from text and links them to DBPedia or Wikidata."
            },
            {
                "role": "user",
                "content": f"Extract named entities from the following description and return the result as a JSON object with 'name' and 'uri' fields. If there is no URI, use null.\n\nDescription: {description}"
            }
        ],
        functions=[
            {
                "name": "extract_named_entities",
                "description": "Extract named entities from text and link them to DBPedia or Wikidata URIs.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "entities": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": "The name of the entity."
                                    },
                                    "uri": {
                                        "type": ["string", "null"],
                                        "description": "The DBPedia or Wikidata URI of the entity, or null if unavailable."
                                    }
                                },
                                "required": ["name", "uri"]
                            }
                        }
                    },
                    "required": ["entities"]
                }
            }
        ],
        function_call={"name": "extract_named_entities"},  # This triggers the function return in JSON
        temperature=0
    )
    
    # Get the function response
    if 'choices' in response and len(response['choices']) > 0:
        function_call = response['choices'][0].get('message', {}).get('function_call', {})
        if function_call and 'arguments' in function_call:
            # Parse the JSON response from the function_call
            arguments = json.loads(function_call['arguments'])
            return arguments.get('entities', [])
    
    return None

# Type hint for an entity dictionary, with 'name' and 'uri' fields
Entity = Dict[str, Optional[str]]

def link_entities_with_article(g: Graph, article: Resource, entities: List[Entity]) -> None:
    for entity in entities:
        entity_name: Optional[str] = entity.get('name')
        entity_uri: Optional[str] = entity.get('uri')
        
        if entity_uri:
            entity_resource = g.resource(URIRef(entity_uri))
            entity_resource.set(RDF.type, SCHEMA.Thing)
            
            if entity_name:
                entity_resource.set(SCHEMA.name, Literal(entity_name))
            
            article.set(SCHEMA.about, entity_resource)


# Example of calling the method with query parameters and API key
query_params = {
    "q": "sparql",
    "from": "2024-09-05",
    "sortBy": "popularity"
}

news_api_key = os.getenv("NEWS_API_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")

news_json = fetch_data(news_api_key, query_params)
articles_graph = to_graph(news_json)

# Iterate over all subjects in the graph that are of type schema:Article
for article_subject in articles_graph.subjects(RDF.type, SCHEMA.Article):
    # Cast each article subject to an RDFLib Resource
    article_resource = Resource(articles_graph, article_subject)

   # Example: Linking extracted entities to this article
    description = article_resource.value(SCHEMA.description)
    if description:
        entities = extract_named_entities(openai_api_key, news_json)
        if entities:
            link_entities_with_article(articles_graph, article, entities)


# Print the RDF output in Turtle format
print(articles_graph.serialize(format='turtle'))
