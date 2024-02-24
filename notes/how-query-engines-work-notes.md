# How Query Engines work (https://howqueryengineswork.com)

## Introduction

Arrow -- in-memory data format + IPC protocol
DataFusion -- query engine
Ballista -- distributed compute project

> "The query engine covered in this book was originally intended to be part of the Ballista project (and was for a while) but as the project evolved, it became apparent that it would make more sense to keep the query engine in Rust and support Java, and other languages, through a UDF mechanism rather than duplicating large amounts of query execution logic in multiple languages."

## What is a Query Engine?

> A query engine is a piece of software that can execute queries against data to produce answers to questions.

> SQL is powerful and widely understood but has limitations in the world of so-called "Big Data," where data scientists often need to mix in custom code with their queries.

> The DataFusion query engine in the Apache Arrow project is also primarily based on the design in this book. Readers who are more interested in Rust than JVM can refer to the DataFusion source code in conjunction with this book.

## Apache Arrow

> Apache Arrow started as a specification for a memory format for columnar data, with implementations in Java and C++. The memory format is efficient for vectorized processing on modern hardware such as CPUs with SIMD (Single Instruction, Multiple Data) support and GPUs.

### Arrow Memory Model

Each column is represented by a single vector holding the raw data along with separate vectors representing null values and offets into the raw data for variable-width types.

See https://arrow.apache.org/docs/format/Columnar.html for details.

### IPC -- inter-process communication

Data can be passed between processes by passing a pointer to the data. The receiving process needs to know how to interpret this data, so an IPC format is defined for exchanging metadata such as schema information. Arrow uses Google Flatbuffers to define the metadata format.

### Compute Kernels

The scope of Apache Arrow has expanded to provide computational libraries for evaluating expressions against the data.

### Arrow Flight Protocol

More recently, Arrow has defined a Flight Protocol for efficiently streaming Arrow Data over the network. Flight is based on gRPC and Google Protocol Buffers.

The Flight protocol defines a FlightService with the following methods:

#### Handshake

Client/Server Request/Response mechanism to establish a token to be used for future operations.

#### ListFlights

Get a list of available streams given a particular criteria. This API allows listing the streams available for consumption.

GetFlightInfo | GetSchema | DoGet | DoPut | DoExchange | DoAction | ListActions

- Arrow Flight SQL?

### Query Engines

#### DataFusion

The Rust implementation oif Arrow contains an in-memory query engine named DataFusion. InfluxData is using this for next generation InfluxDB.

#### Ballista

"Spark in Rust."

Foundational technologies in Ballista.
- Apache Arrow: memory model and type system
- Apache Arrow Flight: IPC protocol
- Apache Arrow Flight SQL: BI tools and JDBC connection to Ballista.
- Google Protocol Buffers: serializing plans.
- Docker: packaging up executors + user-defined code.
- Kubernetes: deployment and management of executor docker containers.

> Ballista was donated to the Arrow project in 2021 and is not ready for production use although it is capable of running a number of queries from the popular TPC-H benchmark with good performance.

> "Ballista is maturing quickly and is now working towards being production ready. See the roadmap for more details."

Roadmap: https://github.com/apache/arrow-ballista/blob/main/ROADMAP.md

##### Maturity?

- Still Andy doing most commits: https://github.com/apache/arrow-ballista/graphs/contributors?from=2023-01-01&to=2024-02-24&type=c
- Only sporadic activity since 2022: https://github.com/apache/arrow-ballista/graphs/code-frequency (also, mostly moves since we have similar additions as deletions)

- Spark?

- Volcano -- an extensible and parallel query evaluation system

## Choosing a Type System

> The first step in building a query engine is to choose a type system to represent the different types of data that the query engine will be processing. One option would be to invent a proprietary type system specific to the query engine. Another option is to use the type system of the data source that the query engine is designed to query from.

> If the query engine is going to support querying multiple data sources, which is often the case, then there is likely some conversion required between each supported data source and the query engine's type system, and it will be important to use a type system capable of representing all the data types of all the supported data sources.

### Row-based or Columnar?

Many of today's query engines are based on the Volcano Query Planner where each step in the physical plan is essentially an iterator over rows. https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf. This is a simple model to implement but tends to introduce per-row overheads that add up pretty quickly when running a query against billions of rows.

### Interoperability

Should we be accessible from multiple programming languages? Maybe ODBC / JDBC?

Use industry-standard: Apache Arrow. (Built by Wes McKinney, creator of Pandas.)

### Type System

We will use Apache Arrow as the basis of our type system.
- Schema: provides metadata for a data source or the results from a query. A schema consists of one of more fields.
- Field provides the name and data type for a field within a schema, and specifies whether it allows nll values or not.
- FieldVector: provides columnar storage for data for a field.
- ArrowType: represents a data type.

#### Digression: Apache Arrow from JavaScript

https://arrow.apache.org/docs/js/

> Apache Arrow is a columnar memory layout specification for encoding vectors and table-like containers of flat and nested data. The Arrow spec aligns columnar data in memory to minimize cache misses and take advantage of the latest SIMD (Single input multiple data) and GPU operations on modern processors.

- https://github.com/domoritz/arrow-tools
- 

