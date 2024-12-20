import argparse
from client import LinkedDataClient

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Fetch RDF data using LinkedDataClient.")
    parser.add_argument("url", help="The URL to fetch RDF data from.")  # Positional argument for the URL
    parser.add_argument("--cert", required=True, help="Path to the certificate .pem file containing both the private key and certificate.")
    parser.add_argument("--password", required=True, help="Password for the encrypted private key in the .pem file.")

    args = parser.parse_args()

    # Initialize the LinkedDataClient
    client = LinkedDataClient(
        cert_pem_path=args.cert,
        cert_password=args.password,
        verify_ssl=False
    )

    # Fetch RDF data
    try:
        graph = client.get(args.url)
        print(f"Fetched {len(graph)} triples:")
        for subj, pred, obj in graph:
            print(f"{subj} {pred} {obj}")
    except RuntimeError as e:
        print(e)
