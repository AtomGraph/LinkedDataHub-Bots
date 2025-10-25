import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import logging
from typing import Dict, Any, ClassVar
from rdflib import Graph, Dataset, Literal, Namespace, BNode, URIRef
from rdflib.namespace import RDF, XSD
from web_algebra.operation import Operation
from web_algebra.mcp_tool import MCPTool
from mcp import types

logger = logging.getLogger(__name__)


# Define Namespaces
SCHEMA = Namespace("http://schema.org/")
ARXIV_NS = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}


class ArxivFeed(Operation, MCPTool):
    """
    Fetches research papers from ArXiv API and converts them to RDF using schema.org vocabulary.
    """

    # ArXiv API endpoint
    API_ENDPOINT: ClassVar[str] = "https://export.arxiv.org/api/query"

    @classmethod
    def description(cls) -> str:
        return "Fetches research papers from ArXiv API based on query parameters and converts them to RDF using schema.org vocabulary."

    @classmethod
    def inputSchema(cls) -> dict:
        return {
            "type": "object",
            "properties": {
                "search_query": {
                    "type": "string",
                    "description": "ArXiv search query (e.g., 'all:RDF OR all:SPARQL')"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return (default: 10)",
                    "default": 10
                },
                "sort_by": {
                    "type": "string",
                    "description": "Sort by: relevance, lastUpdatedDate, or submittedDate (default: submittedDate)",
                    "default": "submittedDate"
                },
                "sort_order": {
                    "type": "string",
                    "description": "Sort order: ascending or descending (default: descending)",
                    "default": "descending"
                }
            },
            "required": ["search_query"]
        }

    def execute(self, search_query: Literal, max_results: Literal = None,
                sort_by: Literal = None, sort_order: Literal = None) -> Dataset:
        """
        Pure function: Fetch ArXiv papers and convert to RDF Dataset

        :param search_query: ArXiv search query
        :param max_results: Maximum number of results
        :param sort_by: Sort field
        :param sort_order: Sort order
        :return: RDF Dataset with each paper in a separate named graph
        """
        # Convert RDFLib Literals to Python values
        search_query_str = str(search_query)
        max_results_int = int(str(max_results)) if max_results else 10
        sort_by_str = str(sort_by) if sort_by else "submittedDate"
        sort_order_str = str(sort_order) if sort_order else "descending"

        # Build query parameters dict
        params = {
            "search_query": search_query_str,
            "sortBy": sort_by_str,
            "sortOrder": sort_order_str,
            "max_results": str(max_results_int)
        }

        # Encode parameters and build URL
        encoded_params = urllib.parse.urlencode(params)
        query_url = f"{self.API_ENDPOINT}?{encoded_params}"

        # Fetch data from ArXiv
        logger.info("Fetching from ArXiv: %s", query_url)
        xml_data = self._fetch_data(query_url)

        # Convert to RDF Graph
        return self._to_graph(xml_data)

    def execute_json(self, arguments: dict, variable_stack: list = []) -> Dataset:
        """JSON execution: process arguments and delegate to execute()"""
        search_query = Operation.process_json(
            self.settings, arguments["search_query"], self.context, variable_stack
        )

        max_results = None
        sort_by = None
        sort_order = None

        if "max_results" in arguments:
            max_results = Operation.process_json(
                self.settings, arguments["max_results"], self.context, variable_stack
            )
        if "sort_by" in arguments:
            sort_by = Operation.process_json(
                self.settings, arguments["sort_by"], self.context, variable_stack
            )
        if "sort_order" in arguments:
            sort_order = Operation.process_json(
                self.settings, arguments["sort_order"], self.context, variable_stack
            )

        return self.execute(search_query, max_results, sort_by, sort_order)

    def mcp_run(self, arguments: dict, context: Any = None) -> list[types.TextContent]:
        """MCP execution: plain args → plain results"""
        search_query = Literal(arguments["search_query"])
        max_results = Literal(arguments.get("max_results", 10))
        sort_by = Literal(arguments.get("sort_by", "submittedDate")) if "sort_by" in arguments else None
        sort_order = Literal(arguments.get("sort_order", "descending")) if "sort_order" in arguments else None

        graph = self.execute(search_query, max_results, sort_by, sort_order)

        # Serialize to Turtle for MCP response
        turtle_str = graph.serialize(format="turtle")
        return [types.TextContent(type="text", text=turtle_str)]

    def _fetch_data(self, query_url: str) -> str:
        """
        Fetch data from ArXiv API

        :param query_url: Full query URL
        :return: XML response as string
        """
        req = urllib.request.Request(query_url)
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8')

    def _to_graph(self, xml_data: str) -> Dataset:
        """
        Convert ArXiv Atom XML to RDF Dataset with each paper in a named graph

        :param xml_data: ArXiv API XML response
        :return: RDF Dataset with each paper as schema.org ScholarlyArticle in separate named graph
        """
        # Create RDF Dataset
        ds = Dataset()

        # Parse XML
        root = ET.fromstring(xml_data)

        # Get all entry elements (papers)
        entries = root.findall("atom:entry", ARXIV_NS)
        logger.info("Processing %d papers from ArXiv", len(entries))

        for entry in entries:
            # Get ArXiv ID first to use as graph name
            arxiv_id_elem = entry.find("atom:id", ARXIV_NS)
            if arxiv_id_elem is None:
                continue

            arxiv_url = arxiv_id_elem.text
            arxiv_id = arxiv_url.split('/abs/')[-1] if '/abs/' in arxiv_url else arxiv_url

            # Create a named graph for this paper
            graph_uri = URIRef(f"http://arxiv.org/paper/{arxiv_id}")
            g = ds.graph(graph_uri)

            # Create a blank node for the paper
            paper = g.resource(BNode())

            # Set paper type as schema.org ScholarlyArticle
            paper.set(RDF.type, SCHEMA.ScholarlyArticle)

            # ArXiv ID and URL
            paper.add(URIRef("http://arxiv.org/property/id"), Literal(arxiv_id))
            paper.set(SCHEMA.url, URIRef(arxiv_url))

            # Title
            title_elem = entry.find("atom:title", ARXIV_NS)
            if title_elem is not None and title_elem.text:
                # ArXiv titles can have newlines, clean them
                title = ' '.join(title_elem.text.split())
                paper.set(SCHEMA.headline, Literal(title))
                paper.set(SCHEMA.name, Literal(title))

            # Abstract (summary)
            summary_elem = entry.find("atom:summary", ARXIV_NS)
            if summary_elem is not None and summary_elem.text:
                abstract = ' '.join(summary_elem.text.split())
                paper.set(SCHEMA.abstract, Literal(abstract))

            # Authors
            for author_elem in entry.findall("atom:author", ARXIV_NS):
                name_elem = author_elem.find("atom:name", ARXIV_NS)
                if name_elem is not None and name_elem.text:
                    author = g.resource(BNode())
                    author.set(RDF.type, SCHEMA.Person)
                    author.set(SCHEMA.name, Literal(name_elem.text))
                    paper.add(SCHEMA.author, author.identifier)

            # Published date
            published_elem = entry.find("atom:published", ARXIV_NS)
            if published_elem is not None and published_elem.text:
                # ArXiv dates are in ISO format: 2024-01-15T10:30:00Z
                paper.set(SCHEMA.datePublished, Literal(published_elem.text, datatype=XSD.dateTime))

            # Updated date
            updated_elem = entry.find("atom:updated", ARXIV_NS)
            if updated_elem is not None and updated_elem.text:
                paper.set(SCHEMA.dateModified, Literal(updated_elem.text, datatype=XSD.dateTime))

            # Categories
            for category_elem in entry.findall("atom:category", ARXIV_NS):
                term = category_elem.get("term")
                if term:
                    paper.add(SCHEMA.keywords, Literal(term))

            # Links (PDF, abstract page)
            for link_elem in entry.findall("atom:link", ARXIV_NS):
                link_type = link_elem.get("type")
                href = link_elem.get("href")
                if href:
                    if link_type == "application/pdf":
                        # PDF link
                        paper.set(URIRef("http://schema.org/encoding"), URIRef(href))
                    elif link_elem.get("rel") == "alternate":
                        # Abstract page link (already set as url above)
                        pass

            # DOI (if available)
            doi_elem = entry.find("arxiv:doi", ARXIV_NS)
            if doi_elem is not None and doi_elem.text:
                paper.set(SCHEMA.sameAs, URIRef(f"https://doi.org/{doi_elem.text}"))

            # Journal reference (if available)
            journal_ref_elem = entry.find("arxiv:journal_ref", ARXIV_NS)
            if journal_ref_elem is not None and journal_ref_elem.text:
                # Create a PublicationIssue for the journal reference
                publication = g.resource(BNode())
                publication.set(RDF.type, SCHEMA.PublicationIssue)
                publication.set(SCHEMA.name, Literal(journal_ref_elem.text))
                paper.set(SCHEMA.isPartOf, publication.identifier)

        return ds
