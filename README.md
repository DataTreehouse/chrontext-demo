# Chrontext Demo — Federated Time-Series Queries

## What is virtualisation?

In the knowledge graph world, *virtualisation* means querying data **where it already
lives** — without copying it into the graph first. Instead of extracting millions of
time-series rows into RDF triples (slow, memory-hungry, and instantly stale), chrontext
leaves the data in DuckDB, PostgreSQL, BigQuery, or an OPC UA server and translates
the relevant parts of your SPARQL query into native database calls on the fly.

Why this is powerful: your knowledge graph captures *what things are and how they relate*
(sensors, stations, grid topology), while your time-series database stores *what happened
and when* (measurements, events, readings). Virtualisation lets you ask questions that
span both in a single query — no ETL pipeline, no stale snapshots, no glue code.
The query engine pushes filters and aggregations down to the database, so only the
results you need ever cross the boundary.

---

This demo shows how maplib's built-in **chrontext** engine lets you write a single
SPARQL query that transparently joins a knowledge graph with a time-series database.

## The scenario

12 Norwegian weather stations — from Blindern in Oslo to Hammerfest in the Arctic.
The setup script fetches **real forecast data** from MET Norway's free
[Locationforecast API](https://api.met.no/weatherapi/locationforecast/2.0/documentation)
(no API key required), giving ~9 days of hourly forecasts per station.

- **Knowledge graph**: Station metadata — name, municipality, type (coastal/mountain/urban),
  geographic coordinates, installed sensors, and their relationships.
- **DuckDB database**: Real weather forecasts — timestamped temperature, wind speed, and
  precipitation data from MET Norway, stored in DuckDB.

The magic of chrontext: **one SPARQL query** can ask questions that span both worlds,
like "show me the average temperature for all coastal stations north of 64°N."

## What's in this folder

```
chrontext-demo/
├── setup_data.py        # Fetches real weather data from api.met.no → DuckDB
├── main.py              # The demo script — builds KG, connects DuckDB, runs query
├── demo.ipynb           # Jupyter notebook with guided walkthrough
├── tpl/
│   └── stations.stottr  # OTTR templates for stations and sensors
├── data/
│   ├── stations.csv     # Station metadata (12 Norwegian stations)
│   └── weather.duckdb   # (generated) time-series database with real MET data
└── queries/
    └── sensor_overview.rq  # Pure KG: station/sensor structure
```

## How to run

```bash
pip install maplib duckdb polars sqlalchemy
python setup_data.py    # fetch real weather data from MET Norway
python main.py          # run the demo
```

No API key needed. The Locationforecast API just requires a proper User-Agent header
(already set in the script). Data is freely available under the
[Norwegian Licence for Open Government Data](https://data.norge.no/nlod/en/2.0).

## The point

With chrontext, you write **one SPARQL query** that spans the knowledge graph and the
time-series database — no manual orchestration, no glue code, no pulling intermediate
results into Python. The engine pushes filters and aggregations down to DuckDB and joins
the results via zero-copy Arrow DataFrames.

In benchmarks published in Expert Systems & Applications, chrontext was 10–85× faster
than Ontop.
