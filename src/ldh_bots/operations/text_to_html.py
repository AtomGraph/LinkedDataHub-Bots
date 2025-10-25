"""Convert plain text to HTML by wrapping paragraphs in <p> tags."""

import logging
from typing import Any, ClassVar
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF
from web_algebra.operation import Operation
from web_algebra.mcp_tool import MCPTool
from mcp import types

logger = logging.getLogger(__name__)

# Define Namespaces
RDF_NS = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")


class TextToHTML(Operation, MCPTool):
    """
    Converts plain text to HTML by splitting on newlines and wrapping paragraphs in <p> tags.
    """

    @classmethod
    def description(cls) -> str:
        return "Converts plain text to HTML by splitting on newlines and wrapping each paragraph in <p> tags."

    @classmethod
    def inputSchema(cls) -> dict:
        return {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Plain text to convert to HTML"
                },
                "strip_empty": {
                    "type": "boolean",
                    "description": "Remove empty paragraphs (default: true)",
                    "default": True
                }
            },
            "required": ["text"]
        }

    def execute(self, text: Literal, strip_empty: Literal = None) -> Literal:
        """
        Pure function: Convert plain text to HTML

        :param text: Plain text to convert
        :param strip_empty: Whether to strip empty paragraphs (default: True)
        :return: RDF Literal with HTML content and rdf:HTML datatype
        """
        # Convert RDFLib Literal to Python string
        text_str = str(text)
        strip_empty_bool = True if strip_empty is None else bool(str(strip_empty).lower() in ['true', '1', 'yes'])

        # Split text by newlines
        lines = text_str.split('\n')

        # Build HTML paragraphs
        paragraphs = []
        for line in lines:
            stripped = line.strip()
            # Skip empty lines if strip_empty is True
            if strip_empty_bool and not stripped:
                continue
            # Wrap in <p> tags
            paragraphs.append(f"<p>{stripped}</p>")

        # Join paragraphs
        html = '\n'.join(paragraphs)

        # Return as RDF Literal with rdf:HTML datatype
        return Literal(html, datatype=RDF_NS.HTML)

    def execute_json(self, arguments: dict, variable_stack: list = []) -> Literal:
        """JSON execution: process arguments and delegate to execute()"""
        # Process text argument
        text = Operation.process_json(
            self.settings, arguments["text"], self.context, variable_stack
        )

        strip_empty = None
        if "strip_empty" in arguments:
            strip_empty = Operation.process_json(
                self.settings, arguments["strip_empty"], self.context, variable_stack
            )

        return self.execute(text, strip_empty)

    def mcp_run(self, arguments: dict, context: Any = None) -> list[types.TextContent]:
        """MCP execution: plain args → plain results"""
        text = Literal(arguments["text"])
        strip_empty = Literal(arguments.get("strip_empty", True)) if "strip_empty" in arguments else None

        result = self.execute(text, strip_empty)

        # Return HTML as text content
        return [types.TextContent(type="text", text=str(result))]
