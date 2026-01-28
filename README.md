# DATA-ENGINEERING-CHALLENGE
## Setup

The pipeline can be executed locally using the provided Makefile.

bash
make install

This command installs all required dependencies and prepares the environment.
Once the setup is complete, the entire pipeline can be run by executing:

python3 energy-pipeline/src/main.py

The main.py script runs the full pipeline end to end, from data extraction to Gold-level outputs.

## Design Decisions
### CSV vs API Date Formats

The input datasets use different timestamp formats:

- CSV source: dates are expressed in a local format such as 12/20/23 14:00, without explicit timezone information.
- API source: timestamps are provided in an ISO-like format (YYYY-MM-DDTHH) and are already in UTC.

To ensure consistency and avoid ambiguity, all timestamps are normalized to UTC during the Silver transformation phase.
The parsing logic is fully configuration-driven: each timestamp column declares its input format and timezone handling rules directly in the YAML configuration files. This keeps the code simple and the behavior explicit.


## Timezone Handling

Data coming from different geographic regions is handled with a single, clear rule:

- All timestamps are converted to UTC in the Silver layer.
- CSV timestamps are initially interpreted as local time, based on the country or region, and then converted to UTC.
- API timestamps are parsed directly as UTC.

Both the input format and the timezone strategy (fixed timezone or derived from a column value) are defined in the Silver YAML configuration. This avoids hard-coded logic and makes timezone behavior transparent and reproducible.

## Raw Staging Area

A dedicated Raw (staging) layer was introduced to store data exactly as received from the source, without any transformation.

CSV files and API JSON responses are copied byte-to-byte.
No type casting, parsing, or normalization is performed at this stage.

This approach prevents early errors or format changes from propagating downstream and guarantees that Bronze and Silver always operate on a stable and reproducible source of truth.


## Parametric and Extensible Design

The entire pipeline is designed to be fully configuration-driven.
Adding a new CSV or API source only requires updating the corresponding YAML file.

Supporting new formats or transformations does not require modifying existing logic, but only adding new configuration entries or dedicated helper functions.
Bronze, Silver, and Gold layers are generic and reusable across datasets.

This design significantly speeds up the onboarding of new data sources while keeping the codebase clean and maintainable.

## Validation and Testing (Planned)

I planned to introduce a set of tests focused on configuration quality and consistency.
At the moment, only an initial draft has been implemented (e.g. validate_raw_config and require_keys) to verify the presence of mandatory configuration keys.

If needed, this can be extended to include:
- structural validation of YAML files
- cross-reference checks between inputs, mappings, and joins
- basic schema sanity checks before execution

## Pipeline Structure

The pipeline is logically split into two main phases:

- Extract: from source systems to the Raw layer
- Transform: from Bronze to Silver and Gold

This separation keeps responsibilities clear and aligns with common data-engineering best practices.

## Future Improvements
### Automated Orchestration

As a next step, an additional orchestration layer (e.g. orchestrator.py) could be introduced on top of main.py to automatically trigger the pipeline every 24 hours.
This was not implemented as part of the challenge, but the current structure fully supports it.

### Configuration Quality Tests

A dedicated test suite could be added to validate configuration files before execution, ensuring early detection of inconsistencies and reducing runtime failures.
