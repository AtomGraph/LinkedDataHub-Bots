"""Script that fetches ArXiv papers and pushes them to LinkedDataHub."""

import warnings
# Suppress Pydantic warning from web_algebra
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic._internal._generate_schema")

import argparse
import urllib.request
import urllib.error
from datetime import datetime
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import rdflib
from rdflib import Literal, Namespace, Dataset, URIRef, BNode, Graph
from rdflib.namespace import RDF, FOAF, XSD
from tqdm import tqdm
from lxml import etree
from ldh_bots.operations.feeds.arxiv import ArxivFeed
from web_algebra.operations.linkeddatahub.create_item import CreateItem
from web_algebra.operations.linkeddatahub.content.add_xhtml_block import AddXHTMLBlock
from web_algebra.operations.linked_data.post import POST

SCHEMA = Namespace("http://schema.org/")
XHTML_NS = "http://www.w3.org/1999/xhtml"
MATHML_NS = "http://www.w3.org/1998/Math/MathML"


def parse_date_to_arxiv_format(date_str: str) -> str:
    """Parse various date formats and convert to arXiv format (YYYYMMDDHHMM).

    Supported input formats:
    - YYYY-MM-DD (time defaults to 00:00)
    - YYYYMMDD (time defaults to 00:00)
    - YYYY-MM-DD HH:MM
    - YYYYMMDDHHMM (native arXiv format, returned as-is)

    Args:
        date_str: Date string in one of the supported formats

    Returns:
        Date in arXiv format (YYYYMMDDHHMM)

    Raises:
        ValueError: If date format is not recognized
    """
    date_str = date_str.strip()

    # Try native arXiv format first (YYYYMMDDHHMM)
    if len(date_str) == 12 and date_str.isdigit():
        return date_str

    # Try YYYYMMDD format
    if len(date_str) == 8 and date_str.isdigit():
        return date_str + "0000"

    # Try YYYY-MM-DD format
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y%m%d0000")
    except ValueError:
        pass

    # Try YYYY-MM-DD HH:MM format
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        return dt.strftime("%Y%m%d%H%M")
    except ValueError:
        pass

    raise ValueError(
        f"Invalid date format: '{date_str}'. "
        f"Supported formats: YYYY-MM-DD, YYYYMMDD, YYYY-MM-DD HH:MM, or YYYYMMDDHHMM"
    )


def fetch_arxiv_html(arxiv_id: str) -> str:
    """Fetch HTML version of ArXiv paper.

    Args:
        arxiv_id: ArXiv ID (e.g., "2510.12134v1")

    Returns:
        HTML content as string, or None if not available
    """
    url = f"https://arxiv.org/html/{arxiv_id}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def extract_sections(html_content: str) -> list[tuple[str, etree._Element, str]]:
    """Extract sections from ArXiv HTML.

    Args:
        html_content: Full HTML document as string

    Returns:
        List of tuples (section_id, section_element, section_title)
    """
    root = etree.fromstring(html_content.encode('utf-8'))
    sections = []

    # Find all section elements
    for section in root.xpath('//section[@id]'):
        section_id = section.get('id')

        # Get section title from first heading
        title_elem = section.find('.//*[@class="ltx_title"]')
        section_title = ""
        if title_elem is not None:
            # Extract text content, removing tags
            section_title = ''.join(title_elem.itertext()).strip()

        sections.append((section_id, section, section_title))

    return sections


def wrap_and_serialize_c14n(section: etree._Element) -> str:
    """Wrap section in XHTML div and serialize as canonical XML.

    Args:
        section: Section element to wrap

    Returns:
        C14N 2.0 serialized XHTML string (required for RDF XMLLiterals)
    """
    from copy import deepcopy
    import unicodedata

    # Create wrapper div with XHTML as default namespace (not prefixed)
    wrapper = etree.Element(f"{{{XHTML_NS}}}div", nsmap={None: XHTML_NS})

    # Deep copy the section
    section_copy = deepcopy(section)
    wrapper.append(section_copy)

    # Use C14N 2.0 (required for RDF XMLLiterals)
    c14n_xml = etree.canonicalize(wrapper, with_comments=False)

    # Normalize Unicode to NFC form (canonical composition)
    c14n_xml = unicodedata.normalize('NFC', c14n_xml)

    return c14n_xml


class Settings(BaseSettings):
    """Settings with LinkedDataHub certificate configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    # Certificate settings for LinkedDataHub
    cert_pem_path: str = Field(default=None, description="Path to PEM certificate file")
    cert_password: str = Field(default=None, description="Password for certificate private key")


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Fetch ArXiv papers and push to LinkedDataHub"
    )
    parser.add_argument(
        "--search-query",
        type=str,
        required=True,
        help="ArXiv search query (e.g., 'all:RDF OR all:SPARQL')"
    )
    parser.add_argument(
        "--container",
        type=str,
        default="https://papers.localhost:4443/arxiv/",
        help="LinkedDataHub container URL (default: https://papers.localhost:4443/arxiv/)"
    )
    parser.add_argument(
        "--cert",
        type=str,
        required=True,
        help="Path to PEM certificate file for LinkedDataHub authentication"
    )
    parser.add_argument(
        "--cert-password",
        type=str,
        required=True,
        help="Password for certificate private key"
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=10,
        help="Maximum number of papers to fetch (default: 10)"
    )
    parser.add_argument(
        "--sort-by",
        type=str,
        default="submittedDate",
        choices=["relevance", "lastUpdatedDate", "submittedDate"],
        help="Sort by field (default: submittedDate)"
    )
    parser.add_argument(
        "--sort-order",
        type=str,
        default="descending",
        choices=["ascending", "descending"],
        help="Sort order (default: descending)"
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        help="Filter papers from this date (formats: YYYY-MM-DD, YYYYMMDD, YYYY-MM-DD HH:MM, or YYYYMMDDHHMM)"
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default=None,
        help="Filter papers up to this date (formats: YYYY-MM-DD, YYYYMMDD, YYYY-MM-DD HH:MM, or YYYYMMDDHHMM)"
    )

    args = parser.parse_args()

    # Create settings object with certificate paths from arguments
    settings = Settings(
        cert_pem_path=args.cert,
        cert_password=args.cert_password
    )

    # LinkedDataHub container URL
    container_url = URIRef(args.container)

    # Construct date filter if provided
    search_query = args.search_query
    if args.from_date or args.to_date:
        try:
            from_date_str = parse_date_to_arxiv_format(args.from_date) if args.from_date else "190001010000"
            to_date_str = parse_date_to_arxiv_format(args.to_date) if args.to_date else "209912312359"

            date_filter = f"submittedDate:[{from_date_str} TO {to_date_str}]"
            # Combine with existing search query using AND
            search_query = f"({args.search_query}) AND {date_filter}"

            print(f"Date filter applied: {date_filter}")
        except ValueError as e:
            print(f"Error parsing date: {e}")
            exit(1)

    # Step 1: Fetch ArXiv papers
    print(f"Searching ArXiv for: '{search_query}'...")
    arxiv_feed = ArxivFeed(settings=settings, context={})
    papers_dataset = arxiv_feed.execute(
        search_query=Literal(search_query),
        max_results=Literal(args.max_results),
        sort_by=Literal(args.sort_by),
        sort_order=Literal(args.sort_order)
    )

    # Get list of named graphs (each graph = one paper)
    paper_graphs = [(graph_uri, papers_dataset.graph(graph_uri))
                    for graph_uri in papers_dataset.graphs()
                    if graph_uri != papers_dataset.default_context.identifier]
    print(f"Found {len(paper_graphs)} papers\n")

    # Initialize operations
    create_item = CreateItem(settings=settings, context={})
    post = POST(settings=settings, context={})
    add_xhtml_block = AddXHTMLBlock(settings=settings, context={})

    paper_count = 0

    # Step 2: Process each paper individually
    print(f"Processing and pushing papers to LinkedDataHub at {container_url}...\n")
    for graph_uri, paper_graph in tqdm(paper_graphs, desc="Processing papers", unit="paper"):
        # Get paper metadata (paper is a blank node in the named graph)
        paper_blank_node = paper_graph.value(predicate=RDF.type, object=SCHEMA.ScholarlyArticle)
        if not paper_blank_node:
            print(f"Skipping graph without article")
            continue

        # Get ArXiv ID for slug
        arxiv_id = paper_graph.value(paper_blank_node, URIRef("http://arxiv.org/property/id"))
        if not arxiv_id:
            print(f"Skipping paper without ArXiv ID")
            continue

        title = paper_graph.value(paper_blank_node, SCHEMA.headline)
        if not title:
            title = paper_graph.value(paper_blank_node, SCHEMA.name)
        if not title:
            print(f"Skipping paper without title")
            continue

        paper_count += 1
        print(f"\n[{paper_count}] Processing: {title}")
        print(f"    ArXiv ID: {arxiv_id}")

        # Step 2a: Create item in LinkedDataHub using ArXiv ID as slug
        try:
            result = create_item.execute(
                container_uri=container_url,
                title=Literal(str(title)),
                slug=Literal(str(arxiv_id))
            )
            item_url = result.bindings[0]["url"]
            print(f"    Created item at: {item_url}")
        except Exception as e:
            print(f"    Error creating item: {e}")
            continue

        # Step 2b: Create proper paper URI as item_url + "#this"
        paper_node = URIRef(str(item_url) + "#this")
        print(f"    Paper URI: {paper_node}")

        # Step 2c: Replace blank node with proper URI in the paper's graph
        for p, o in list(paper_graph.predicate_objects(paper_blank_node)):
            paper_graph.remove((paper_blank_node, p, o))
            paper_graph.add((paper_node, p, o))

        for s, p in list(paper_graph.subject_predicates(paper_blank_node)):
            paper_graph.remove((s, p, paper_blank_node))
            paper_graph.add((s, p, paper_node))

        # Add foaf:primaryTopic linking the document to the article
        paper_graph.add((URIRef(str(item_url)), FOAF.primaryTopic, paper_node))

        # Step 2d: POST the paper graph to LinkedDataHub
        try:
            print(f"    Pushing paper graph ({len(paper_graph)} triples)...")
            post_result = post.execute(
                url=URIRef(item_url),
                data=paper_graph
            )

            status = post_result.bindings[0]["status"]
            print(f"    POST status: {status}")

            # Step 2e: Fetch and add HTML sections if available
            print(f"    Fetching HTML version for {arxiv_id}...")
            html_content = fetch_arxiv_html(str(arxiv_id))

            if html_content:
                print(f"    Extracting sections from HTML...")
                sections = extract_sections(html_content)
                print(f"    Found {len(sections)} sections")

                # Add each section as XHTML block
                for section_id, section_elem, section_title in sections:
                    try:
                        print(f"      Adding section {section_id}: {section_title[:50]}...")

                        # Wrap and serialize as C14N
                        xhtml_content = wrap_and_serialize_c14n(section_elem)

                        # Add XHTML block to LinkedDataHub
                        try:
                            # Workaround for RDFLib bug: Disable literal normalization to prevent
                            # minidom.toxml() from converting empty elements like <div></div> to <div/>
                            # which violates C14N and breaks Apache Jena 4.7.0 XMLLiteral validation
                            # See: https://github.com/RDFLib/rdflib/blob/main/rdflib/term.py#L1930
                            rdflib.NORMALIZE_LITERALS = False
                            xml_literal = Literal(xhtml_content, datatype=RDF.XMLLiteral)

                            add_xhtml_block.execute(
                                url=URIRef(item_url),
                                value=xml_literal,
                                title=Literal(section_title, datatype=XSD.string) if section_title else None,
                                fragment=Literal(section_id, datatype=XSD.string)
                            )

                            rdflib.NORMALIZE_LITERALS = True
                        except urllib.error.HTTPError as e:
                            error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
                            print(f"      Error adding section {section_id}: HTTP {e.code}")
                            print(f"      Error body: {error_body[:200]}...")
                            continue
                        except Exception as e:
                            print(f"      Error adding section {section_id}: {e}")
                            continue
                    except Exception as e:
                        print(f"      Error processing section {section_id}: {e}")
                        continue
            else:
                print(f"    HTML version not available for {arxiv_id}")

        except Exception as e:
            print(f"    Error posting data: {e}")
            continue

    print(f"\nDone! Processed and pushed {paper_count} papers to LinkedDataHub.")
