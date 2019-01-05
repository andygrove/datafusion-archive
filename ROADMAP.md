# DataFusion Roadmap

Note that this roadmap is subject to very frequent changes.

## 0.6.0 - Feature parity with original POC

This release will support simple queries (project, selection, aggregates, sorting) against CSV and Parquet files.

- [ ] Upgrade to Apache Arrow 0.12.0
- [ ] Parquet support (local file system)
- [ ] Implement simple aggregate queries (min/max/count/sum) with optional GROUP BY
- [ ] Implement ORDER BY (in-memory)
- [ ] Support `CREATE EXTERNAL TABLE` SQL to register data sources

## 0.7.0 - Mature query execution

The goal of this release is to support a larger percentage of real world queries.

- [ ] Scalar UDFs
- [ ] Array UDFs
- [ ] StructArray (nested objects + dot notation)
- [ ] JOIN support

## 0.8.0 - Distributed queries

This release will allow queries to be executed against a cluster, supporting interactive queries that return results in Arrow format or write results to disk.

- [ ] Serializable physical query plan (protobuf)
- [ ] Worker node that can receive and execute plan (single threaded) against local files
- [ ] Return query result in Arrow format
- [ ] Write query output to local files (in DataFusion and/or Arrow format)
- [ ] Distributed query planner
- [ ] Worker can delegate portions of query plan to other workers
- [ ] Data source meta-data
- [ ] Kubernetes support for spinning up worker nodes
- [ ] JDBC driver

## 0.9.0 - Performance optimizations

- [ ] Query optimizer improvements
- [ ] Parallel execution using threads (async/await)
- [ ] Delegate to Apache Arrow / Gandiva for query execution using LLVM
- [ ] Spill to disk for SORT/JOIN/AGGREGATE queries

## 1.0.0 - Usability

- [ ] Web user interface / better tools / monitoring etc

## Backlog / TBD

- [ ] Support for S3
- [ ] Support for HDFS
- [ ] Authentication/authorization
- [ ] Encryption at rest and in transit
