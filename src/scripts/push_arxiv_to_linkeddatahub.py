"""Script that fetches ArXiv papers and pushes them to LinkedDataHub."""

import warnings
# Suppress Pydantic warning from web_algebra
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic._internal._generate_schema")

import argparse
from urllib.parse import quote
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from rdflib import Literal, Namespace, Dataset, URIRef, BNode
from rdflib.namespace import RDF, FOAF
from tqdm import tqdm
from ldh_bots.operations.feeds.arxiv import ArxivFeed
from web_algebra.operations.linkeddatahub.create_item import CreateItem
from web_algebra.operations.linked_data.post import POST

SCHEMA = Namespace("http://schema.org/")


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
        default="https://papers.localhost:4443/papers/",
        help="LinkedDataHub container URL (default: https://papers.localhost:4443/papers/)"
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

    args = parser.parse_args()

    # Create settings object with certificate paths from arguments
    settings = Settings(
        cert_pem_path=args.cert,
        cert_password=args.cert_password
    )

    # LinkedDataHub container URL
    container_url = URIRef(args.container)

    # Step 1: Fetch ArXiv papers
    print(f"Searching ArXiv for: '{args.search_query}'...")
    arxiv_feed = ArxivFeed(settings=settings, context={})
    papers_dataset = arxiv_feed.execute(
        search_query=Literal(args.search_query),
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

    paper_count = 0

    # Step 2: Process each paper individually
    print(f"Processing and pushing papers to LinkedDataHub at {container_url}...\n")
    for graph_uri, paper_graph in tqdm(paper_graphs, desc="Processing papers", unit="paper"):
        # Get paper metadata (paper is a blank node in the named graph)
        paper_blank_node = paper_graph.value(predicate=RDF.type, object=SCHEMA.ScholarlyArticle)
        if not paper_blank_node:
            print(f"Skipping graph without article")
            continue

        title = paper_graph.value(paper_blank_node, SCHEMA.headline)
        if not title:
            title = paper_graph.value(paper_blank_node, SCHEMA.name)
        if not title:
            print(f"Skipping paper without title")
            continue

        paper_count += 1
        print(f"\n[{paper_count}] Processing: {title}")

        # Step 2a: Create item in LinkedDataHub to get the item URL
        slug = quote(str(title).lower().replace(" ", "-"), safe="")
        try:
            result = create_item.execute(
                container_uri=container_url,
                title=Literal(str(title)),
                slug=Literal(slug)
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

        except Exception as e:
            print(f"    Error posting data: {e}")
            continue

    print(f"\nDone! Processed and pushed {paper_count} papers to LinkedDataHub.")
