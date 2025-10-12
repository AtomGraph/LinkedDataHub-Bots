"""Main script that fetches news and extracts entities - composition of operations."""

import warnings
# Suppress Pydantic warning from web_algebra
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic._internal._generate_schema")

from pydantic_settings import BaseSettings
from rdflib import Literal, Namespace
from rdflib.namespace import RDF
from ldh_bots.operations.news_api_feed import NewsAPIFeed
from ldh_bots.operations.extract_entities import ExtractEntities
from ldh_bots.operations.reconcile_dbpedia import ReconcileDBPedia
from ldh_bots.operations.reconcile_wikidata import ReconcileWikidata
from web_algebra.operations.merge import Merge

SCHEMA = Namespace("http://schema.org/")


if __name__ == "__main__":
    # Create settings object
    settings = BaseSettings()

    # Step 1: Fetch news articles
    print("Fetching news articles about 'SPARQL'...")
    news_feed = NewsAPIFeed(settings=settings, context={})
    articles_graph = news_feed.execute(
        keyword=Literal("sparql"),
        articles_count=Literal(3),
        date_start=Literal("2025-10-01"),
        lang=Literal("eng")
    )
    print(f"Fetched articles\n")

    # Step 2: Extract entities from each article's text
    print("Extracting entities from articles...")
    extract_entities = ExtractEntities(settings=settings, context={})

    entity_graphs = []
    # Iterate over articles and extract entities from each
    for article_node in articles_graph.subjects(RDF.type, SCHEMA.Article):
        article_body = articles_graph.value(article_node, SCHEMA.articleBody)
        if article_body:
            # Extract entities from this article's text and link them to the article
            entities_graph = extract_entities.execute(
                text=article_body,
                article=article_node,
                entity_types=["Person", "Organization", "Place"]
            )
            entity_graphs.append(entities_graph)

    # Step 3: Merge all graphs together (articles + entities)
    print(f"Merging {len(entity_graphs)} entity graphs with articles graph...")
    merge = Merge(settings=settings, context={})
    all_graphs = [articles_graph] + entity_graphs
    enriched_graph = merge.execute(all_graphs)

    # Step 4: Reconcile entities with DBpedia (adds owl:sameAs links)
    print("\nReconciling entities with DBpedia...")
    reconcile_dbpedia = ReconcileDBPedia(settings=settings, context={})
    dbpedia_graph = reconcile_dbpedia.execute(enriched_graph)
    print(f"DBpedia reconciliation complete")

    # Step 5: Reconcile entities with Wikidata (adds owl:sameAs links)
    print("Reconciling entities with Wikidata...")
    reconcile_wikidata = ReconcileWikidata(settings=settings, context={})
    final_graph = reconcile_wikidata.execute(dbpedia_graph)
    print(f"Wikidata reconciliation complete")

    # Write the enriched and reconciled RDF to file
    print("\nWriting final graph to sparql-news-with-entities.ttl...")
    with open("sparql-news-with-entities.ttl", "w") as f:
        f.write(final_graph.serialize(format='turtle'))
    print("Done!")
