# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LinkedDataHub-Bots is a Python library for bot implementations that push RDF data to LinkedDataHub. The core functionality is built around client-certificate authenticated HTTPS requests for RDF data operations.

## Architecture

### Core Components

**LinkedDataClient (`client.py`)**: The main client class that handles all RDF operations with LinkedDataHub.
- Uses SSL/TLS client certificate authentication (PEM format with password-protected private key)
- Supports optional SSL verification bypass (`verify_ssl=False` parameter)
- All RDF data is serialized/parsed as N-Triples format
- HTTP methods: `get()`, `post()`, `put()`, `delete()`
- Built on Python's `urllib.request` with custom SSL context

**load_rdf.py**: Example CLI script demonstrating how to use LinkedDataClient to fetch RDF data.

### Key Design Patterns

1. **SSL Context Configuration**: Client certificate and SSL verification settings are configured once during client initialization
2. **RDF Serialization**: All RDF data is handled as RDFLib Graph objects and serialized to/from N-Triples
3. **HTTP Headers**: Content negotiation uses `application/n-triples` for both Accept and Content-Type headers

## Development Commands

### Setup
```bash
uv sync
```

### Running the Example Script
```bash
uv run load-rdf <url> --cert <path-to-pem> --password <cert-password>
```

Or run directly with Python:
```bash
uv run python load_rdf.py <url> --cert <path-to-pem> --password <cert-password>
```

## Important Implementation Notes

- **Bug in client.py**: Lines 60 and 78 reference undefined variable `data` instead of parameter `graph` - these should be `graph.serialize(format="nt")`
- Client certificates must be in PEM format with both private key and certificate in the same file
- The private key can be password-protected (password provided during client initialization)
- SSL verification can be disabled for development/testing environments using `verify_ssl=False`
