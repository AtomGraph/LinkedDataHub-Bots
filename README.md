# LinkedDataHub-Bots

Bot implementations that push RDF data to LinkedDataHub

## Installation

Install dependencies using [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

## Usage

### Using the CLI script

Fetch RDF data from a LinkedDataHub instance:

```bash
uv run load-rdf <url> --cert <path-to-pem> --password <cert-password>
```

### Using the Python API

```python
from client import LinkedDataClient

# Initialize client with certificate authentication
client = LinkedDataClient(
    cert_pem_path="path/to/cert.pem",
    cert_password="your-password",
    verify_ssl=False  # Optional: disable SSL verification for development
)

# Fetch RDF data
graph = client.get("https://example.com/resource")

# Post RDF data
client.post("https://example.com/container", graph)

# Update RDF data
client.put("https://example.com/resource", graph)

# Delete RDF data
client.delete("https://example.com/resource")
```

## Features

- Client certificate authentication (PEM format with password-protected private keys)
- Full CRUD operations (GET, POST, PUT, DELETE) for RDF resources
- RDF data handling using RDFLib Graph objects
- N-Triples serialization format
- Optional SSL verification bypass for development environments

## License

Apache License 2.0
