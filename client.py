import ssl
import urllib.request
import argparse
from rdflib import Graph

# Set up argument parsing
parser = argparse.ArgumentParser(description="Make an HTTPS request with RDF response handling and no SSL verification.")
parser.add_argument("url", help="The URL to send the request to")  # Positional argument for the URL
parser.add_argument("--cert", required=True, help="Path to the certificate .pem file containing both the private key and certificate")
parser.add_argument("--password", required=True, help="Password for the encrypted private key in the .pem file")

args = parser.parse_args()

# Read arguments
cert_pem_path = args.cert
cert_password = args.password
url = args.url

# Create an SSL context that does not verify the server's certificate
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
ssl_context.load_cert_chain(certfile=cert_pem_path, password=cert_password)

# Use the SSL context in an HTTPS handler
https_handler = urllib.request.HTTPSHandler(context=ssl_context)
opener = urllib.request.build_opener(https_handler)

# Create a request with the Accept header for N-Triples
headers = {"Accept": "application/n-triples"}
request = urllib.request.Request(url, headers=headers)

try:
    # Perform the request
    response = opener.open(request)
    
    # Read and decode the response
    data = response.read().decode("utf-8")
    
    # Parse the response using RDFLib
    g = Graph()
    g.parse(data=data, format="nt")
    
    # Print the parsed triples
    for subj, pred, obj in g:
        print(f"{subj} {pred} {obj}")
    
    print(f"\nParsed {len(g)} triples.")
    
except Exception as e:
    print(f"An error occurred: {e}")
