"""
Chrontext Demo — Federated Time-Series Queries with maplib

Uses real weather forecast data from MET Norway (api.met.no).
Run setup_data.py first to fetch data and generate the DuckDB database.
"""

from maplib import (
    Model, Prefix, Variable, Template, Parameter, RDFType, Triple,
    VirtualizedDatabase, xsd,
)
from sqlalchemy import MetaData, Table, Column, select, literal_column
import polars as pl
import duckdb

ns     = "http://weather.treehouse.example/"
ns_tpl = "http://weather.treehouse.example/tpl/"
ct_ns  = "https://github.com/DataTreehouse/chrontext#"


# -----------------------------------------------------------
# Build the knowledge graph
# -----------------------------------------------------------

model = Model()
model.add_template(open("tpl/stations.stottr").read())

df_stations = pl.read_csv("data/stations.csv")
df_stations = df_stations.with_columns(
    (pl.lit(ns) + pl.col("station_id")).alias("station_uri"),
    (pl.lit(ns + "municipality/") + pl.col("municipality")).alias("municipality_uri"),
    (pl.lit(ns + "StationType") + pl.col("station_type").str.to_titlecase()).alias("station_type_uri"),
    pl.col("installed_year").cast(pl.String).alias("installed_year_str"),
)

model.map(ns_tpl + "Station", df_stations.select([
    "station_uri", "name", "municipality_uri", "latitude", "longitude",
    "station_type_uri", "elevation_m", "installed_year_str",
]).rename({
    "municipality_uri": "municipality",
    "station_type_uri": "station_type",
    "installed_year_str": "installed_year",
}))

# Add rdfs:label for municipality and station type IRIs so they have
# human-readable names in query results.
muni_labels = df_stations.select(
    (pl.lit(ns + "municipality/") + pl.col("municipality")).alias("subject"),
    pl.col("municipality").alias("object"),
).unique()
model.map_triples(muni_labels, predicate="http://www.w3.org/2000/01/rdf-schema#label")

type_labels = df_stations.select(
    (pl.lit(ns + "StationType") + pl.col("station_type").str.to_titlecase()).alias("subject"),
    pl.col("station_type").alias("object"),
).unique()
model.map_triples(type_labels, predicate="http://www.w3.org/2000/01/rdf-schema#label")

measurands = ["temperature", "wind_speed", "precipitation"]

# Define measurand IRIs with type, label, and unit.
measurand_metadata = {
    "temperature":   {"iri": "MeasurandTemperature",   "label": "Temperature",   "unit": "unit:DEG_C"},
    "wind_speed":    {"iri": "MeasurandWindSpeed",      "label": "Wind Speed",     "unit": "unit:M-PER-SEC"},
    "precipitation": {"iri": "MeasurandPrecipitation",  "label": "Precipitation",  "unit": "unit:MilliM"},
}

# Map measurand metadata into the graph
measurand_triples = []
for name, meta in measurand_metadata.items():
    uri = f"{ns}{meta['iri']}"
    measurand_triples.append({"subject": uri, "object": f"{ns}Measurand"})
model.map_triples(
    pl.DataFrame(measurand_triples),
    predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
)

measurand_labels = []
measurand_units = []
for name, meta in measurand_metadata.items():
    uri = f"{ns}{meta['iri']}"
    measurand_labels.append({"subject": uri, "object": meta["label"]})
    measurand_units.append({"subject": uri, "object": meta["unit"]})
model.map_triples(
    pl.DataFrame(measurand_labels),
    predicate="http://www.w3.org/2000/01/rdf-schema#label",
)
model.map_triples(
    pl.DataFrame(measurand_units),
    predicate=f"{ns}hasUnit",
)

sensor_rows = []
for row in df_stations.iter_rows(named=True):
    for measurand in measurands:
        sensor_rows.append({
            "station_uri": row["station_uri"],
            "sensor_uri":  f"{ns}{row['station_id']}_sensor_{measurand}",
            "measurand":   f"{ns}{measurand_metadata[measurand]['iri']}",
        })

model.map(ns_tpl + "Sensor", pl.DataFrame(sensor_rows))

# Link each sensor to its time-series via chrontext.
# Each sensor needs an intermediate node with three predicates:
#   sensor  → ct:hasTimeseries → ts_node
#   ts_node → ct:hasExternalId → "ST001_sensor_temperature"  (matches SQL id)
#   ts_node → ct:hasResource   → "temperature"               (matches resource_sql_map key)
ts_link_rows, ts_extid_rows, ts_resource_rows = [], [], []
for row in df_stations.iter_rows(named=True):
    for measurand in measurands:
        sensor_id = f"{row['station_id']}_sensor_{measurand}"
        ts_node = f"{ns}ts/{sensor_id}"
        ts_link_rows.append({"subject": f"{ns}{sensor_id}", "object": ts_node})
        ts_extid_rows.append({"subject": ts_node, "object": sensor_id})
        ts_resource_rows.append({"subject": ts_node, "object": measurand})

model.map_triples(pl.DataFrame(ts_link_rows),     predicate=f"{ct_ns}hasTimeseries")
model.map_triples(pl.DataFrame(ts_extid_rows),    predicate=f"{ct_ns}hasExternalId")
model.map_triples(pl.DataFrame(ts_resource_rows), predicate=f"{ct_ns}hasResource")

print(f"Knowledge graph: {model.size()} triples ({len(df_stations)} stations, {len(sensor_rows)} sensors)")


# -----------------------------------------------------------
# Connect DuckDB via chrontext virtualization
# -----------------------------------------------------------

class WeatherDuckDB:
    def __init__(self, path):
        self.con = duckdb.connect(path, read_only=True)
        self.con.execute("SET TimeZone = 'UTC'")

    def query(self, sql: str) -> pl.DataFrame:
        return self.con.execute(sql).pl()

db = WeatherDuckDB("data/weather.duckdb")

metadata = MetaData()
measurements = Table("measurements", metadata,
    Column("station_id"), Column("timestamp"),
    Column("temperature"), Column("wind_speed"), Column("precipitation"),
)

def make_resource_sql(measurand: str):
    return select(
        measurements.c.timestamp,
        measurements.c[measurand].label("value"),
    ).select_from(measurements).add_columns(
        literal_column(f"(measurements.station_id || '_sensor_{measurand}')").label("id"),
    )

vdb = VirtualizedDatabase(
    database=db,
    resource_sql_map={name: make_resource_sql(name) for name in measurands},
    sql_dialect="postgres",
)

ct = Prefix("https://github.com/DataTreehouse/chrontext#")

def make_ts_template(name: str) -> Template:
    id_var, timestamp_var, value_var, dp_var = (
        Variable("id"), Variable("timestamp"), Variable("value"), Variable("dp")
    )
    return Template(
        iri=ct.suf(f"{name}TimeSeries"),
        parameters=[
            Parameter(variable=id_var,        rdf_type=RDFType.Literal(xsd.string)),
            Parameter(variable=timestamp_var, rdf_type=RDFType.Literal(xsd.dateTime)),
            Parameter(variable=value_var,     rdf_type=RDFType.Literal(xsd.double)),
        ],
        instances=[
            Triple(id_var, ct.suf("hasDataPoint"), dp_var),
            Triple(dp_var, ct.suf("hasValue"),     value_var),
            Triple(dp_var, ct.suf("hasTimestamp"), timestamp_var),
        ],
    )

model.add_virtualization(
    virtualized_database=vdb,
    resources={name: make_ts_template(name.title().replace("_", "")) for name in measurands},
)
print("Chrontext virtualization added\n")


# -----------------------------------------------------------
# One federated SPARQL query
# -----------------------------------------------------------

result = model.query("""
    PREFIX wx:   <http://weather.treehouse.example/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX ct:   <https://github.com/DataTreehouse/chrontext#>

    SELECT ?name ?muni_name (AVG(?temp) AS ?avg_temp)
                            (MIN(?temp) AS ?min_temp)
                            (MAX(?temp) AS ?max_temp)
    WHERE {
        ?station a wx:WeatherStation ;
                 rdfs:label      ?name ;
                 wx:municipality ?muni ;
                 wx:stationType  wx:StationTypeCoastal ;
                 wx:hasSensor    ?sensor .
        ?muni    rdfs:label      ?muni_name .
        ?sensor  wx:measurand    wx:MeasurandTemperature .

        ?sensor ct:hasTimeseries ?ts .
        ?ts ct:hasDataPoint ?dp .
        ?dp ct:hasTimestamp ?t ;
            ct:hasValue     ?temp .
    }
    GROUP BY ?name ?muni_name
    ORDER BY ?avg_temp
""")

print("Coastal stations — temperature statistics from one federated SPARQL query:")
print(result)
