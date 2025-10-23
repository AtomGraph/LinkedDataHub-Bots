import os
import urllib.request
import json
import logging
from typing import Dict, Any, ClassVar
from rdflib import Graph, Literal, Namespace, BNode, URIRef
from rdflib.namespace import RDF, XSD, OWL
from web_algebra.operation import Operation
from web_algebra.mcp_tool import MCPTool
from mcp import types

logger = logging.getLogger(__name__)


# Define Namespaces
SCHEMA = Namespace("http://schema.org/")


class NewsAPIArticle(Operation, MCPTool):
    """
    Fetches a single news article from NewsAPI.ai by URI and converts it to RDF using schema.org vocabulary.
    """

    # NewsAPI.ai endpoint
    API_ARTICLE_DETAILS_ENDPOINT: ClassVar[str] = "http://eventregistry.org/api/v1/article/getArticle"

    @classmethod
    def description(cls) -> str:
        return "Fetches a single news article from NewsAPI.ai by URI and converts it to RDF using schema.org vocabulary."

    @classmethod
    def inputSchema(cls) -> dict:
        return {
            "type": "object",
            "properties": {
                "article_uri": {
                    "type": "string",
                    "description": "NewsAPI.ai article URI (e.g., '8920966279')"
                }
            },
            "required": ["article_uri"]
        }

    def execute(self, article_uri: Literal) -> Graph:
        """
        Pure function: Fetch single article data and convert to RDF Graph

        :param article_uri: NewsAPI.ai article URI
        :return: RDF Graph with single article
        """
        # Get API key from settings
        api_key_str = getattr(self.settings, 'news_api_key', None) or os.getenv("NEWS_API_KEY")
        if not api_key_str:
            raise ValueError("NEWS_API_KEY not found in settings or environment")

        # Convert RDFLib Literal to Python value
        article_uri_str = str(article_uri)

        # Fetch article details
        article_data = self._fetch_article_details(article_uri_str, api_key_str)

        # Convert to RDF Graph
        return self._article_to_graph(article_data)

    def execute_json(self, arguments: dict, variable_stack: list = []) -> Graph:
        """JSON execution: process arguments and delegate to execute()"""
        article_uri = Operation.process_json(
            self.settings, arguments["article_uri"], self.context, variable_stack
        )
        return self.execute(article_uri)

    def mcp_run(self, arguments: dict, context: Any = None) -> list[types.TextContent]:
        """MCP execution: plain args → plain results"""
        article_uri = Literal(arguments["article_uri"])
        graph = self.execute(article_uri)

        # Serialize to Turtle for MCP response
        turtle_str = graph.serialize(format="turtle")
        return [types.TextContent(type="text", text=turtle_str)]

    def _fetch_article_details(self, article_uri: str, api_key: str) -> Dict:
        """
        Fetch full article details including concepts, location, and categories

        :param article_uri: Article URI from NewsAPI.ai
        :param api_key: NewsAPI.ai API key
        :return: Full article details JSON
        """
        request_payload = {
            "articleUri": article_uri,
            "apiKey": api_key,
            "resultType": "info",
            "infoArticleBodyLen": -1,
            "includeArticleConcepts": True,
            "includeArticleCategories": True,
            "includeArticleLocation": True,
            "includeArticleExtractedDates": True,
            "includeArticleImage": True
        }

        # Prepare the request
        data = json.dumps(request_payload).encode('utf-8')
        req = urllib.request.Request(
            self.API_ARTICLE_DETAILS_ENDPOINT,
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        # Fetch and process the JSON data
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            # Extract the article info from the response
            # Response format: {"article_uri": {"info": {...}}}
            if article_uri in result:
                return result[article_uri].get('info', {})
            return {}

    def _article_to_graph(self, article_data: Dict) -> Graph:
        """
        Convert single article JSON to RDF Graph

        :param article_data: NewsAPI.ai article JSON data
        :return: RDF Graph with article as schema.org Article resource
        """
        # Create RDF Graph
        g = Graph()

        # Create a blank node for the article
        article = g.resource(BNode())

        # Set article type as schema.org Article
        article.set(RDF.type, SCHEMA.Article)

        # Set properties on the RDFLib Resource
        if "title" in article_data and article_data['title']:
            article.set(SCHEMA.headline, Literal(article_data['title']))

        if "body" in article_data and article_data['body']:
            # NewsAPI.ai provides full article body
            article.set(SCHEMA.articleBody, Literal(article_data['body']))

        # Authors in NewsAPI.ai can be a list
        if "authors" in article_data and article_data['authors']:
            authors = article_data['authors']
            if isinstance(authors, list):
                for author_data in authors:
                    if "name" in author_data and author_data['name']:
                        article.add(SCHEMA.author, Literal(author_data['name']))
            elif isinstance(authors, str):
                article.set(SCHEMA.author, Literal(authors))

        if "url" in article_data and article_data['url']:
            article.set(SCHEMA.url, URIRef(article_data['url']))

        # NewsAPI.ai uses different date formats
        if "date" in article_data and article_data['date']:
            article.set(SCHEMA.datePublished, Literal(article_data['date'], datatype=XSD.date))
        elif "dateTime" in article_data and article_data['dateTime']:
            article.set(SCHEMA.datePublished, Literal(article_data['dateTime'], datatype=XSD.dateTime))

        # Add source information
        if "source" in article_data and article_data['source']:
            source_data = article_data['source']
            source = g.resource(BNode())
            source.set(RDF.type, SCHEMA.Organization)

            if "title" in source_data and source_data['title']:
                source.set(SCHEMA.name, Literal(source_data['title']))

            if "uri" in source_data and source_data['uri']:
                source.set(SCHEMA.url, URIRef(f"http://eventregistry.org/{source_data['uri']}"))

            # Link the source (publisher) to the article
            article.set(SCHEMA.publisher, source)

        # Add image if available
        if "image" in article_data and article_data['image']:
            article.set(SCHEMA.image, URIRef(article_data['image']))

        # Add location as contentLocation (spatial context of the article)
        # schema:contentLocation is better than schema:location (which is deprecated in schema.org)
        if "location" in article_data and article_data['location']:
            location_data = article_data['location']
            if location_data.get('label'):
                # Create Place entity
                place = g.resource(BNode())
                place.set(RDF.type, SCHEMA.Place)

                # Add place name
                if 'eng' in location_data['label']:
                    place.set(SCHEMA.name, Literal(location_data['label']['eng']))

                # Add country if available
                if 'country' in location_data and location_data['country'].get('label'):
                    country_label = location_data['country']['label'].get('eng')
                    if country_label:
                        place.set(SCHEMA.addressCountry, Literal(country_label))

                # Link place to article via schema:contentLocation
                article.set(SCHEMA.contentLocation, place._identifier)

        return g
