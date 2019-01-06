# DataFusion Roadmap

## 0.5.x - Feature parity with original POC (in progress)

I recently updated the code to use my fork of Arrow and this required considerable rework so I am currently working on restoring functionality that was previously working.

- [x] Upgrade to Apache Arrow 0.12.0
- [x] Allow query to be executed against Arrow CSV reader
- [ ] Allow query to be executed against Arrow Parquet reader
- [x] Logical query plan definition
- [x] SQL Parser
- [x] Query planner
- [x] Projection
- [x] Selection
- [x] Simple aggregate queries with optional GROUP BY
- [x] Support for MIN/MAX
- [ ] Support for SUM
- [ ] Support for COUNT
- [ ] Support for COUNT(DISTINCT)
- [ ] ORDER BY
- [ ] Support `CREATE EXTERNAL TABLE` SQL to register data sources
- [ ] SQL console and Docker image for standalone use / easy testing and benchmarking


## 0.6.0 - Mature query execution

The goal of this release is to support a larger percentage of real world queries and to focus on improved unit testing to ensure correctness of query execution.

- Scalar UDFs
- Array UDFs
- Support nested objects with dot notation
- JOIN support (hash join and sort merge join)
- Better unit tests / smoke test / performance tests

## 0.7.0 - Parallel processing / partitions

- Parallel execution using threads (async/await)
- Partitioning
- Query optimizer improvements

## 0.8.0 - Groundwork for distributed queries

- Serializable logical query plan (in protobuf format)
- Worker node that can receive and execute plan against local files
- Write query output to local files or return results in protobuf and/or IPC format
- Consider supporting Hive protocol to allow JDBC/ODBC clients to submit queries to a single node

## 0.9.0 - Distributed queries

This release will allow queries to be executed against a cluster, supporting interactive queries that return results in Arrow format or write results to disk.

- Distributed query planner
- Worker can delegate portions of query plan to other workers
- Data source meta-data
- Kubernetes support for spinning up worker nodes

## 1.0.0 - Usability

- Web user interface / better tools / monitoring etc

## Backlog / TBD

- Support for S3
- Support for HDFS
- Authentication/authorization
- Encryption at rest and in transit
