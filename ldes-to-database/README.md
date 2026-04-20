# Read from an LDES server and store events in a database

In this tutorial we'll have a look at reading an LDES server and putting all events into a
database.
This is an invaluable use case, as many clients want to store the data from an (external) LDES and
store this data into their own database for further processing or integrating with their own
systems and applications.

## First, set the stage

In this tutorial, we are reusing the same public parking occupancy endpoint introduced in
`advanced-conversion`. Concretely, that means we keep the same data source (the City of Ghent
real-time parking occupancy API), but we now focus on a different outcome: instead of only
publishing Linked Data Event Stream members, we set up an end-to-end flow where data is ingested,
normalized, versioned, and made available for downstream persistence in a database-backed setup.

Why reuse that endpoint? It gives us a realistic stream of frequently changing state data, which is
exactly what LDES is good at: representing evolving entities as immutable, timestamped versions. In
our case, the workbench periodically polls the JSON endpoint, receives the latest occupancy
snapshot,
and transforms each parking facility record into RDF. During this conversion, the pipeline maps
source fields to a linked-data model, then creates version objects so updates over time become
explicit members in the stream. Finally, those members are sent to the local OpenLDES server.

So the architecture in this example is straightforward and practical:

1. A poller in the workbench calls the external JSON endpoint every two minutes.
2. An RML adapter converts JSON records into intermediate RDF.
3. A SPARQL transformer maps that RDF to the target model used in this tutorial.
4. A versioning step generates stable versioned members based on modification time.
5. The pipeline outputs those members to the local `occupancy` event stream on the LDES server.

To start the stage locally, bring up the stack with Docker Compose from this folder:

```bash
docker compose --file docker-compose-ldes-server.yaml up -d --wait 
```

This launches the required runtime components for this demo: a Postgres database that is needed for
the LDES server, the OpenLDES server, and the workbench. Once they are running, the LDES server is
available to host your event stream, and the workbench is ready to run the polling/conversion
pipeline. Normally, you would need to add the ldes stream and configuration, and the pipeline
yourself by executing some HTTP POST requests to the LDES server and workbench. But, as this is not
the main focus of this tutorial, we already set everything up, so you're good to go. If you're
interested in the details, you can check the `init-ldes-server` service in the
docker-compose-ldes-server.yaml file, which contains the commands to set up the LDES stream and
views, and the `init-workbench` service in the docker-compose-ldes-client.yaml file.

As soon as the pipeline is active, it starts downloading the public parking data, converts the
incoming data, creates versions, and pushes members into the local LDES stream.

In short, you set up an LDES server, which gets populated with the public parking data. We need this
component for downstream database consumption, which we'll do next.

> **Note** We're using http://localhost:8080 here for the LDES server, but you'll
> see http://host.docker.internal:8080 in the ldes server configuration and in the responses from
> the server. This is because the LDES client, which we'll set up later, runs in a separate Docker
> network, and needs to access the server from within its own container. The `host.docker.internal`
> hostname is a special DNS name that resolves to the internal IP address of the host machine,
> allowing containers to access services running on the host.


After a while (wait for at least 5 minutes), when data is flowing into the LDES server, you can
check the members in the stream by sending a GET request to `http://localhost:8080/occupancy`:

```bash
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy"
```

There are four views defined on the stream, so you can also check the members in each view by
sending a GET request to the view endpoint, for example:

* By page: `http://localhost:8080/occupancy/by-page`
* By time: `http://localhost:8080/occupancy/by-time`
* By location: `http://localhost:8080/occupancy/by-location`
* By parking: `http://localhost:8080/occupancy/by-parking`

You can navigate all views by following the links to the actual pages, for example:

Navigate by page:

```bash
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-page"
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-page?pageNumber=1"
```

Navigate by time:

```bash
YEAR=$(date +%Y)
MONTH=$(date +%m)
DAY=$(date +%d)
HOUR=$(TZ=UTC date +%H)

curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-time"
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-time?year=$YEAR"
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-time?year=$YEAR&month=$MONTH"
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-time?year=$YEAR&month=$MONTH&day=$DAY"
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-time?year=$YEAR&month=$MONTH&day=$DAY&hour=$HOUR"
curl -H "content-type: text/turtle" "http://localhost:8080/occupancy/by-time?year=$YEAR&month=$MONTH&day=$DAY&hour=$HOUR&pageNumber=1"
```

> **Note** It's possible that the by-time view is not working as expected, due to the fact that
> the data source is in a different timezone than the server. If you encounter issues with the
> by-time view, you can check the timestamps of the members in the stream and adjust the time
> parameters accordingly.

## Setting up the LDES client pipeline

Now that the LDES server is running and populated with parking occupancy data, we can set up a
client that consumes the stream and stores the data in a relational database. This is the core of
this tutorial: showing how LDES data can flow into a traditional PostgreSQL database for
querying, reporting, or integration with existing applications.

The architecture of the client side is simple:

1. A **PostgreSQL database** holds the parking occupancy data in a single `occupancy` table.
2. A **client workbench** runs an LDES client pipeline that reads from the LDES server, extracts
   the relevant fields from each member using a SPARQL SELECT query, and writes the results to the
   database using the `LdioRdbOut` component.

### Docker Compose setup

The client stack is defined in `docker-compose-ldes-client.yaml` and contains two services:

* **client-postgresdb** — A PostgreSQL instance with a database called `occupancy`, accessible on
  port `5432`. On first startup, it runs the `init-db.sql` script (mounted into
  `/docker-entrypoint-initdb.d/`) to create the target table.
* **client-workbench** — An LDIO (Linked Data Interactions Orchestrator) instance that
  automatically loads the pipeline definition from `client/ldes-to-db-pipeline.yaml`. It connects
  to the database using the Spring datasource configured in `client/application.yml`. The workbench
  depends on the database, so Docker ensures the database is available before starting the
  pipeline.

### Database init script

The file `client/init-db.sql` creates the `occupancy` table that will hold the extracted parking
data:

```sql
CREATE TABLE IF NOT EXISTS occupancy
(
    parking        VARCHAR(512) PRIMARY KEY,
    type           VARCHAR(256),
    label          VARCHAR(256),
    is_version_of  VARCHAR(512),
    modified       TIMESTAMPTZ,
    total_capacity INT,
    current_value  INT,
    operator       VARCHAR(256),
    free_of_charge BOOLEAN,
    url            VARCHAR(512),
    opening_hours  VARCHAR(256),
    latitude       NUMERIC(10, 5),
    longitude      NUMERIC(10, 5)
);
```

Each column corresponds to a variable in the SPARQL SELECT query (explained below). The `parking`
URI serves as the primary key, and the data types are chosen to match the expected values:
timestamps with time zone for modification dates, integers for capacities, double precision for
geographic coordinates, and boolean for free_of_charge.

### What is a SPARQL SELECT query?

SPARQL is the standard query language for RDF data — think of it as SQL for linked data. A
`SELECT` query picks specific variables out of an RDF graph and returns them as a tabular result
set (rows and columns), much like a SQL `SELECT` statement. Each variable (prefixed with `?`) in
the `SELECT` clause becomes a column in the output. The `WHERE` clause defines graph patterns that
the RDF data must match, and `OPTIONAL` blocks allow fields to be missing without discarding the
entire row.

In the context of the `LdioRdbOut` component, the SPARQL SELECT query is executed against each
incoming LDES member. The result rows are then inserted into the configured database table, with
each SPARQL variable mapped to the corresponding column.

### The SPARQL SELECT query in the pipeline

The query in `client/ldes-to-db-pipeline.yaml` extracts all relevant parking information from each
LDES member:

```sparql
SELECT ?parking ?type ?label ?is_version_of ?modified ?total_capacity ?current_value 
      ?operator ?free_of_charge ?url ?opening_hours ?latitude ?longitude
WHERE {
    ?parking rdf:type ?type .
    FILTER (?type IN (mobivoc:ParkingLot, mobivoc:ParkingGarage))

    OPTIONAL { ?parking rdfs:label ?label . }
    OPTIONAL { ?parking terms:isVersionOf ?is_version_of . }
    OPTIONAL { ?parking terms:modified ?modified . }
    OPTIONAL {
        ?parking mobivoc:capacity ?totalCap .
        ?totalCap rdf:type mobivoc:Capacity ;
        mobivoc:totalCapacity ?total_capacity .
    }
    OPTIONAL {
        ?parking mobivoc:capacity ?rtCap .
        ?rtCap rdf:type mobivoc:RealTimeCapacity ;
        mobivoc:currentValue ?current_value .
    }
    OPTIONAL {
        ?parking mobivoc:operatedBy ?op .
        ?op rdfs:label ?operator .
    }
    OPTIONAL {
        ?parking mobivoc:price ?price .
        ?price mobivoc:freeOfCharge ?foc .
        BIND(xsd:boolean(?foc) AS ?free_of_charge)
    }
    OPTIONAL { ?parking mobivoc:url ?url . }
    OPTIONAL {
        ?parking schema:openingHoursSpecification ?ohs .
        ?ohs rdfs:label ?opening_hours .
    }
    OPTIONAL { 
        ?parking wgs84_pos:lat ?lat .
        BIND(xsd:double(?lat) AS ?latitude)
    }
    OPTIONAL { 
        ?parking wgs84_pos:long ?long . 
        BIND(xsd:double(?long) AS ?longitude)
    }
}
ORDER BY ?label ?modified
```

Here is what the query does, step by step:

* **`?parking rdf:type ?type`** — Finds every resource, and binds it to the variable `?parking`.
* **`FILTER`** — Keeps only resources that are a `ParkingLot` or `ParkingGarage`, filtering out
  unrelated nodes in the RDF graph.
* **`OPTIONAL` blocks** — Each optional block tries to extract a specific property. If the property
  is not present on a given parking resource, the variable is left unbound (i.e., `NULL` in the
  database) rather than excluding the entire row. This covers:
    - `rdfs:label` — the human-readable name of the parking facility - maps to the `label` column
    - `terms:isVersionOf` — the stable identifier this version belongs to maps to the
      `is_version_of` column
    - `terms:modified` — the timestamp of this version - maps to the `modified` column
    - `mobivoc:Capacity / totalCapacity` — the total number of parking spaces - maps to the
      `total_capacity` column
    - `mobivoc:RealTimeCapacity / currentValue` — the current number of occupied spaces - maps to
      the `current_value` column
    - `mobivoc:operatedBy` — the operator's name - maps to the `operator` column
    - `mobivoc:price / freeOfCharge` — whether parking is free - maps to the `free_of_charge` column
    - `mobivoc:url` — a link to more information - maps to the `url` column
    - `schema:openingHoursSpecification` — opening hours description - maps to the `opening_hours`
      column
    - `wgs84_pos:lat` and `wgs84_pos:long` — geographic coordinates - map to the `latitude` and
      `longitude` columns

* **`ORDER BY ?label ?modified`** — Sorts results alphabetically by name, then chronologically by
  modification time.

The `ignore-duplicate-key-exception: true` setting ensures that if a member with the same
`?parking` URI is received again, the duplicate is silently ignored instead of causing an error. In
our setup, this happens when we restart the LDIO because there's no state repository for the
LDES client. In a production setup, you would typically have a state repository (Postgres) that
stores the last processed member, so you would not encounter duplicates on restart.

```bash
docker compose --file docker-compose-ldes-client.yaml up -d --wait 
```

This command starts the client stack, which includes the PostgreSQL database and the workbench with
the LDES client pipeline. The pipeline will automatically connect to the LDES server, read the
members, execute the SPARQL SELECT query for each member, and insert the results into the
`occupancy` table in the database.

### Check the database

You can connect to the PostgreSQL database to check the contents of the `occupancy` table using the
running Docker container:

```bash
docker compose --file docker-compose-ldes-client.yaml exec client-postgresdb \
  psql -U dbuser -d occupancy -c "SELECT parking, label, modified, total_capacity, current_value, latitude, longitude FROM occupancy LIMIT 10;"
```

This runs `psql` inside the `client-postgresdb` container and queries the first 10 rows from the
`occupancy` table.

> **Note** This table is viewed in a "vi"-style pager, so you can scroll through the results. You
> can exit the pager by pressing `q`.

You can adjust the query to explore the data further, for example:

To count the total number of records:

```bash
docker compose --file docker-compose-ldes-client.yaml exec client-postgresdb \
  psql -U dbuser -d occupancy -c "SELECT COUNT(*) FROM occupancy;"
```

To see all columns for a specific parking facility:

```bash
docker compose --file docker-compose-ldes-client.yaml exec client-postgresdb \
  psql -U dbuser -d occupancy -c "SELECT * FROM occupancy LIMIT 1;"
```

To open an interactive `psql` session:

```bash
docker compose --file docker-compose-ldes-client.yaml exec -it client-postgresdb \
  psql -U dbuser -d occupancy
```

### Data retrieval - views

So we see that the data from the LDES server is now stored in our database. All members will be
stored in the same table, but we don't know which view is used to access the data from the LDES
server.

In fact, the LDES client component will fetch the metadata of the LDES
stream (http://host.docker.internal:8080/occupancy), and will pick one view. We don't have real
control over which view is used.

Sometimes, we want to have control over which view is used, for example, because we want to use the
by-time view to be sure that the members are stored in a time-ordered fashion. To do this, you can
specify the view URL in the pipeline configuration, for example:

```yaml
input:
  name: Ldio:LdesClient
  config:
    urls:
      - http://host.docker.internal:8080/occupancy/by-time
    sourceFormat: text/turtle
    retries:
      enabled: true
```

In this way we are sure that the client will read from the by-time view, and the members will be
processed in a time-ordered fashion. This is especially important if you want to maintain a correct
history of changes in your database, as the by-time view ensures that updates are processed in the
order they were made. However, once the pipeline's state changes from "REPLICATING" to "
SYNCHRONISING", new rows will be inserted in the database as they are added to the different
fragments and pages of the view. So, if a member has a timestamp (in our example the
`ldes:timestampPath` is set to `dct:modified`, so if the member has a `dct:modified` property) that
is older than the last processed member, it will be inserted in the database after all previous
rows.

Do you want to see the result of the by-parking view? You can specify the by-parking view URL in the
pipeline configuration, and then check the database again to see the order of the entries in the
database. First, change the pipeline configuration to use the by-parking view:

```yaml
input:
  name: Ldio:LdesClient
  config:
    urls:
      - http://host.docker.internal:8080/occupancy/by-parking
    sourceFormat: text/turtle
    retries:
      enabled: true
```

Then, stop the client stack and start it again to empty the database and apply the new
configuration:

```bash
docker compose --file docker-compose-ldes-client.yaml down
docker compose --file docker-compose-ldes-client.yaml up -d --wait
```

Then, check the database again to see the order of the entries:

```bash
docker compose --file docker-compose-ldes-client.yaml exec client-postgresdb \
  psql -U dbuser -d occupancy -c "SELECT parking, label, modified, total_capacity, current_value, latitude, longitude FROM occupancy LIMIT 10;"
```

You'll see that the first entries are ordered by parking facility, and not by modification time:

```
                                                     parking                                                      |  label   |        modified        | total_capacity | current_value | latitude | longitude 
------------------------------------------------------------------------------------------------------------------+----------+------------------------+----------------+---------------+----------+-----------
 https://stad.gent/nl/loop/mobiliteit-loop#Parkeerterreinen_Stad_Gent#2026-04-20T17:38:12+02:00                   | The Loop | 2026-04-20 15:38:12+00 |           2490 |          2299 | 51.02542 |   3.68646
 https://stad.gent/nl/loop/mobiliteit-loop#Parkeerterreinen_Stad_Gent#2026-04-20T17:40:41+02:00                   | The Loop | 2026-04-20 15:40:41+00 |           2490 |          2304 | 51.02542 |   3.68646
 https://stad.gent/nl/loop/mobiliteit-loop#Parkeerterreinen_Stad_Gent#2026-04-20T17:44:22+02:00                   | The Loop | 2026-04-20 15:44:22+00 |           2490 |          2307 | 51.02542 |   3.68646
 https://stad.gent/nl/loop/mobiliteit-loop#Parkeerterreinen_Stad_Gent#2026-04-20T17:46:43+02:00                   | The Loop | 2026-04-20 15:46:43+00 |           2490 |          2311 | 51.02542 |   3.68646
 https://stad.gent/nl/mobiliteit-openbare-werken/parkeren/parkings-gent/parking-ramen#2026-04-20T17:38:15+02:00   | Ramen    | 2026-04-20 15:38:15+00 |            254 |           138 | 51.05532 |   3.71653
 https://stad.gent/nl/mobiliteit-openbare-werken/parkeren/parkings-gent/parking-ramen#2026-04-20T17:40:43+02:00   | Ramen    | 2026-04-20 15:40:43+00 |            254 |           142 | 51.05532 |   3.71653
 https://stad.gent/nl/mobiliteit-openbare-werken/parkeren/parkings-gent/parking-ramen#2026-04-20T17:44:25+02:00   | Ramen    | 2026-04-20 15:44:25+00 |            254 |           143 | 51.05532 |   3.71653
 https://stad.gent/nl/mobiliteit-openbare-werken/parkeren/parkings-gent/parking-ramen#2026-04-20T17:46:51+02:00   | Ramen    | 2026-04-20 15:46:51+00 |            254 |           144 | 51.05532 |   3.71653
 https://stad.gent/nl/mobiliteit-openbare-werken/parkeren/parkings-gent/parking-tolhuis#2026-04-20T17:38:14+02:00 | Tolhuis  | 2026-04-20 15:38:14+00 |            155 |            86 | 51.06370 |   3.72497
 https://stad.gent/nl/mobiliteit-openbare-werken/parkeren/parkings-gent/parking-tolhuis#2026-04-20T17:39:27+02:00 | Tolhuis  | 2026-04-20 15:39:27+00 |            155 |            85 | 51.06370 |   3.72497
```

> **Note** When you see only one entry per parking facility, it means that the LDES server 
> has little historical data yet. Take a coffee, relax, and execute those commands again to remove
> the database and restart the client stack:
> ```bash
> docker compose --file docker-compose-ldes-client.yaml down
> docker compose --file docker-compose-ldes-client.yaml up -d --wait
> ```

### Clean up
To stop all running containers and remove the created resources, you can run the following command from the root of this repository:

```bash
docker compose --file docker-compose-ldes-server.yaml down -v
docker compose --file docker-compose-ldes-client.yaml down -v
```


### Next steps

This tutorial demonstrates a simple end-to-end flow from an LDES server to a relational database. In
a real-world application, you would likely want to add more features, such as:

* **State management** — Use a state repository to track the last processed member and avoid
  duplicates on restart.
  See [Persistence strategies](https://openldes.github.io/Linked-Data-Interactions/latest/ldio/ldio-inputs/ldio-ldes-client#persistence-strategies)
  in the LDIO documentation for more details.
* **Data enrichment** — Add additional transformations or lookups to enrich the data before storing
  it. You could use one of the many
  available [LDIO transformers](https://openldes.github.io/Linked-Data-Interactions/latest/ldio/ldio-transformers/index).
  There's also a tutorial on
  that: [Publishing as a standard linked open data model](../advanced-conversion/README.md)
* **Security** - externalize database credentials using environment variables or secrets management:
  see [Using properties in pipelines](https://openldes.github.io/Linked-Data-Interactions/latest/ldio/pipeline-management/management-of-pipelines#using-properties-in-pipelines)
  for more information
* **Security** - secure the LDES server and client communication using authentication and
  encryption.
