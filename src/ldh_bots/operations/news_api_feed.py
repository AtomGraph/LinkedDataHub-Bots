import os
import urllib.request
import json
import logging
from typing import Dict, Any, ClassVar
from datetime import datetime
from rdflib import Graph, Literal, Namespace, BNode, URIRef
from rdflib.namespace import RDF, XSD, OWL
from web_algebra.operation import Operation
from web_algebra.mcp_tool import MCPTool
from mcp import types

logger = logging.getLogger(__name__)


# Define Namespaces
SCHEMA = Namespace("http://schema.org/")


class NewsAPIFeed(Operation, MCPTool):
    """
    Fetches a list of news articles from NewsAPI.ai (Event Registry) search endpoint.
    Returns basic article metadata without fetching full details.
    Use NewsAPIArticle to fetch full details for specific articles.
    """

    # NewsAPI.ai endpoint
    API_ENDPOINT: ClassVar[str] = "http://eventregistry.org/api/v1/article/getArticles"

    @classmethod
    def description(cls) -> str:
        return "Fetches news articles from NewsAPI.ai based on query parameters and converts them to RDF using schema.org vocabulary."

    @classmethod
    def inputSchema(cls) -> dict:
        return {
            "type": "object",
            "properties": {
                "keyword": {
                    "type": "string",
                    "description": "Keywords or phrases to search for in articles"
                },
                "articles_count": {
                    "type": "integer",
                    "description": "Number of articles to return (default: 10)",
                    "default": 10
                },
                "date_start": {
                    "type": "string",
                    "description": "Start date in YYYY-MM-DD format"
                },
                "date_end": {
                    "type": "string",
                    "description": "End date in YYYY-MM-DD format"
                },
                "lang": {
                    "type": "string",
                    "description": "Language code (e.g., 'eng' for English)"
                }
            },
            "required": ["keyword"]
        }

    def execute(self, keyword: Literal, articles_count: Literal = None,
                date_start: Literal = None, date_end: Literal = None, lang: Literal = None) -> Graph:
        """
        Pure function: Fetch news data and convert to RDF Graph

        :param keyword: Search keyword
        :param articles_count: Number of articles to return
        :param date_start: Start date filter
        :param date_end: End date filter
        :param lang: Language filter
        :return: RDF Graph with news articles
        """
        # Get API key from settings
        api_key_str = getattr(self.settings, 'news_api_key', None) or os.getenv("NEWS_API_KEY")
        if not api_key_str:
            raise ValueError("NEWS_API_KEY not found in settings or environment")

        # Convert RDFLib Literals to Python values
        keyword_str = str(keyword)

        # Build request payload
        request_payload = {
            "keyword": keyword_str,
            "apiKey": api_key_str,
            "articlesPage": 1,
            "articlesCount": int(str(articles_count)) if articles_count else 10,
            "articlesSortBy": "date",
            "articlesSortByAsc": False,
            "articlesArticleBodyLen": -1,  # Get full article body
            "resultType": "articles",
            "dataType": ["news", "pr"]
        }

        # Add optional parameters
        if date_start:
            request_payload["dateStart"] = str(date_start)
        if date_end:
            request_payload["dateEnd"] = str(date_end)
        if lang:
            request_payload["lang"] = str(lang)

        # Fetch data from NewsAPI.ai
        news_json = self._fetch_data(request_payload)

        # Convert to RDF Graph using NewsAPIArticle for each article
        return self._to_graph(news_json)

    def execute_json(self, arguments: dict, variable_stack: list = []) -> Graph:
        """JSON execution: process arguments and delegate to execute()"""
        # Process arguments through Operation.process_json to handle nested operations
        keyword = Operation.process_json(
            self.settings, arguments["keyword"], self.context, variable_stack
        )

        articles_count = None
        date_start = None
        date_end = None
        lang = None

        if "articles_count" in arguments:
            articles_count = Operation.process_json(
                self.settings, arguments["articles_count"], self.context, variable_stack
            )
        if "date_start" in arguments:
            date_start = Operation.process_json(
                self.settings, arguments["date_start"], self.context, variable_stack
            )
        if "date_end" in arguments:
            date_end = Operation.process_json(
                self.settings, arguments["date_end"], self.context, variable_stack
            )
        if "lang" in arguments:
            lang = Operation.process_json(
                self.settings, arguments["lang"], self.context, variable_stack
            )

        return self.execute(keyword, articles_count, date_start, date_end, lang)

    def mcp_run(self, arguments: dict, context: Any = None) -> list[types.TextContent]:
        """MCP execution: plain args → plain results"""
        keyword = Literal(arguments["keyword"])
        articles_count = Literal(arguments.get("articles_count", 10))
        date_start = Literal(arguments.get("date_start")) if "date_start" in arguments else None
        date_end = Literal(arguments.get("date_end")) if "date_end" in arguments else None
        lang = Literal(arguments.get("lang")) if "lang" in arguments else None

        graph = self.execute(keyword, articles_count, date_start, date_end, lang)

        # Serialize to Turtle for MCP response
        turtle_str = graph.serialize(format="turtle")
        return [types.TextContent(type="text", text=turtle_str)]

    def _fetch_data(self, request_payload: Dict) -> Dict:
        """
        Fetch data from NewsAPI.ai

        :param request_payload: Request payload dict
        :return: JSON response from NewsAPI.ai
        """
        if not request_payload.get('apiKey'):
            raise ValueError("API key is not provided.")

        # Prepare the request
        data = json.dumps(request_payload).encode('utf-8')
        req = urllib.request.Request(
            self.API_ENDPOINT,
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        # Fetch and process the JSON data
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())

    def _to_graph(self, data: Dict) -> Graph:
        """
        Convert NewsAPI.ai list endpoint response to RDF Graph.
        Only includes basic metadata from the search results, not full article details.

        :param data: NewsAPI.ai JSON response from list endpoint
        :return: RDF Graph with basic article metadata
        """
        # Create RDF Graph
        g = Graph()

        # NewsAPI.ai returns articles in articles.results
        articles = data.get('articles', {}).get('results', [])

        logger.info("Processing %d articles from list endpoint", len(articles))

        # Loop through articles and create basic metadata
        for article_data in articles:
            # Create a blank node for the article
            article = g.resource(BNode())

            # Set article type as schema.org Article
            article.set(RDF.type, SCHEMA.Article)

            # Add NewsAPI.ai URI as custom property (so we can fetch full details later)
            if "uri" in article_data and article_data['uri']:
                article.add(URIRef("http://eventregistry.org/property/uri"), Literal(article_data['uri']))

            # Set basic properties from list endpoint
            if "title" in article_data and article_data['title']:
                article.set(SCHEMA.headline, Literal(article_data['title']))

            if "url" in article_data and article_data['url']:
                article.set(SCHEMA.url, URIRef(article_data['url']))

            # NewsAPI.ai uses different date formats
            if "date" in article_data and article_data['date']:
                article.set(SCHEMA.datePublished, Literal(article_data['date'], datatype=XSD.date))
            elif "dateTime" in article_data and article_data['dateTime']:
                article.set(SCHEMA.datePublished, Literal(article_data['dateTime'], datatype=XSD.dateTime))

            # Add source information if available
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

        return g
