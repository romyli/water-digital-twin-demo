# Water Digital Twin Demo

## Overview
Industry demo built on Databricks showcasing a water utility digital twin use case. Based on a real customer engagement — all content must be fully anonymized.

## Anonymization Rules (CRITICAL)
- **No real customer names** — use fictional names (e.g., "Water Utilities") if a company name is needed
- **No real person names** — use fake names or roles only
- **No identifying details** — obscure locations, contract values, or any detail that could identify the customer
- When the user shares raw notes, treat them as confidential source material. Extract the technical patterns and requirements, but never commit identifying information.

## Project Structure
- `notes/` — local-only directory for raw customer notes (gitignored, never committed)
- `prompts/` — plan and build prompts for the multi-agent demo build

## Tech Stack
- Platform: Databricks
- Language: Python
