"""Main script that fetches news, extracts entities, reconciles them, and pushes to LinkedDataHub."""

import warnings
# Suppress Pydantic warning from web_algebra
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic._internal._generate_schema")

import argparse
from urllib.parse import quote
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from rdflib import Literal, Namespace, Graph, URIRef
from rdflib.namespace import RDF
from tqdm import tqdm
from ldh_bots.operations.news_api_feed import NewsAPIFeed
from ldh_bots.operations.news_api_article import NewsAPIArticle
from ldh_bots.operations.extract_and_reconcile_openai import ExtractAndReconcileOpenAI
from web_algebra.operations.merge import Merge
from web_algebra.operations.linkeddatahub.create_item import CreateItem
from web_algebra.operations.linked_data.post import POST

SCHEMA = Namespace("http://schema.org/")


class Settings(BaseSettings):
    """Settings with LinkedDataHub certificate configuration."""
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    # Certificate settings for LinkedDataHub
    cert_pem_path: str = Field(default=None, description="Path to PEM certificate file")
    cert_password: str = Field(default=None, description="Password for certificate private key")

    # API keys
    openai_api_key: str = Field(default=None)
    news_api_key: str = Field(default=None)


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Fetch news articles, extract entities, reconcile them, and push to LinkedDataHub"
    )
    parser.add_argument(
        "--keyword",
        type=str,
        required=True,
        help="Keyword to search for in news articles"
    )
    parser.add_argument(
        "--container",
        type=str,
        default="https://news.localhost:4443/articles/",
        help="LinkedDataHub container URL (default: https://news.localhost:4443/articles/)"
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
        "--articles-count",
        type=int,
        default=3,
        help="Number of articles to fetch (default: 3)"
    )
    parser.add_argument(
        "--date-start",
        type=str,
        default="2025-10-01",
        help="Start date for news search (YYYY-MM-DD, default: 2025-10-01)"
    )
    parser.add_argument(
        "--lang",
        type=str,
        default="eng",
        help="Language code for news articles (default: eng)"
    )

    args = parser.parse_args()

    # Create settings object with certificate paths from arguments
    settings = Settings(
        cert_pem_path=args.cert,
        cert_password=args.cert_password
    )

    # LinkedDataHub container URL
    container_url = URIRef(args.container)

    # Step 1: Fetch news articles list (basic metadata only)
    print(f"Searching for news articles about '{args.keyword}'...")
    news_feed = NewsAPIFeed(settings=settings, context={})
    feed_graph = news_feed.execute(
        keyword=Literal(args.keyword),
        articles_count=Literal(args.articles_count),
        date_start=Literal(args.date_start),
        lang=Literal(args.lang)
    )

    # Extract article URIs from the feed graph
    ER_URI = URIRef("http://eventregistry.org/property/uri")
    article_uris = []
    for article_node in feed_graph.subjects(RDF.type, SCHEMA.Article):
        article_uri = feed_graph.value(article_node, ER_URI)
        if article_uri:
            article_uris.append(str(article_uri))

    print(f"Found {len(article_uris)} articles\n")

    # Initialize operations
    news_api_article = NewsAPIArticle(settings=settings, context={})
    extract_and_reconcile = ExtractAndReconcileOpenAI(settings=settings, context={})
    merge = Merge(settings=settings, context={})
    create_item = CreateItem(settings=settings, context={})
    post = POST(settings=settings, context={})

    has_reconciled_locations = False
    article_count = 0

    # Step 2: Process each article individually
    print(f"Processing and pushing articles to LinkedDataHub at {container_url}...\n")
    for article_uri in tqdm(article_uris, desc="Processing articles", unit="article"):
        # Step 2a: Fetch full article details
        print(f"\n[{article_count + 1}] Fetching article {article_uri}...")
        article_graph = news_api_article.execute(Literal(article_uri))

        # Get article node from graph
        article_node = article_graph.value(predicate=RDF.type, object=SCHEMA.Article)
        if not article_node:
            print(f"    Skipping: Could not find article node")
            continue

        headline = article_graph.value(article_node, SCHEMA.headline)
        if not headline:
            print(f"    Skipping: No headline found")
            continue

        article_count += 1
        print(f"    Processing: {headline}")

        # Step 2b: Extract and reconcile entities from article body
        entity_graphs = []
        article_body = article_graph.value(article_node, SCHEMA.articleBody)
        if article_body:
            print(f"    Extracting entities...")
            entities_graph = extract_and_reconcile.execute(
                text=article_body,
                article=article_node,
                entity_types=["Person", "Organization", "Place"]
            )
            entity_graphs.append(entities_graph)

        # Step 2c: Reconcile contentLocation if it exists
        location_graphs = []
        location_node = article_graph.value(article_node, SCHEMA.contentLocation)
        if location_node:
            place_name = article_graph.value(location_node, SCHEMA.name)
            place_country = article_graph.value(location_node, SCHEMA.addressCountry)
            print(f"    Raw location from NewsAPI: name='{place_name}', country='{place_country}'")

            # Use name if available, otherwise fall back to country
            location_to_reconcile = place_name if place_name else place_country

            if location_to_reconcile:
                print(f"    Reconciling location: {location_to_reconcile}")
                reconciled_graph = extract_and_reconcile.execute(
                    text=Literal(str(location_to_reconcile)),
                    article=article_node,
                    entity_types=["Place"]
                )

                # Remove old contentLocation triples (unreconciled place)
                article_graph.remove((article_node, SCHEMA.contentLocation, location_node))
                # Remove all triples about the old location node
                for p, o in article_graph.predicate_objects(location_node):
                    article_graph.remove((location_node, p, o))

                # Add contentLocation using ALL owl:sameAs URIs from reconciled Place
                from rdflib.namespace import OWL
                for reconciled_place in reconciled_graph.subjects(RDF.type, SCHEMA.Place):
                    # Add all owl:sameAs values as contentLocation (both DBpedia and Wikidata)
                    for same_as_uri in reconciled_graph.objects(reconciled_place, OWL.sameAs):
                        article_graph.add((article_node, SCHEMA.contentLocation, same_as_uri))
                        print(f"    Added contentLocation: {same_as_uri}")

                    # Remove this Place from schema:mentions (since it's now in contentLocation)
                    reconciled_graph.remove((article_node, SCHEMA.mentions, reconciled_place))
                    # Remove all triples about this Place entity
                    for p, o in list(reconciled_graph.predicate_objects(reconciled_place)):
                        reconciled_graph.remove((reconciled_place, p, o))

                location_graphs.append(reconciled_graph)
                has_reconciled_locations = True

        # Step 2d: Merge article + entities + location
        all_graphs = [article_graph] + entity_graphs + location_graphs
        merged_graph = merge.execute(all_graphs)

        # Print reconciled location graph if it exists
        if location_graphs:
            print(f"    Reconciled location graph (Turtle format):")
            print(f"    {'-'*76}")
            for line in merged_graph.serialize(format="turtle").split('\n'):
                if line.strip():
                    print(f"    {line}")
            print(f"    {'-'*76}")

        # Step 2e: Create item in LinkedDataHub
        slug = quote(str(headline).lower().replace(" ", "-"), safe="")
        try:
            result = create_item.execute(
                container_uri=container_url,
                title=Literal(str(headline)),
                slug=Literal(slug)
            )

            item_url = result.bindings[0]["url"]
            print(f"    Created item at: {item_url}")

            # Step 2f: POST the merged graph to LinkedDataHub
            print(f"    Pushing article graph ({len(merged_graph)} triples)...")
            post_result = post.execute(
                url=item_url,
                data=merged_graph
            )

            status = post_result.bindings[0]["status"]
            print(f"    POST status: {status}")

        except Exception as e:
            print(f"    Error: {e}")
            continue

    print(f"\nDone! Processed and pushed {article_count} articles to LinkedDataHub.")
    if has_reconciled_locations:
        print(f"✓ Reconciled locations with DBpedia/Wikidata")
