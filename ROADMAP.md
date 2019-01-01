# DataFusion Roadmap

## 0.6.0 - Foundations for distributed query support

This release will allow queries to be executed against a single node, supporting interactive queries that return results in Arrow format or write results to disk.

- [ ] Upgrade to Apache Arrow 0.12.0
- [ ] Parquet support (local file system)
- [ ] Serializable logical or physical query plan (protobuf)
- [ ] Worker node that can receive and execute plan (single threaded) against local files
- [ ] Return query output in Arrow format
- [ ] Write query output to local files (in DataFusion and/or Arrow format)
- [ ] Kubernetes support for spinning up worker nodes
- [ ] JDBC driver

## 0.7.0 - Mature query execution

The goal of this release is to support a large percentage of real world queries.

- [ ] Parallel execution using threads (async/await)
- [ ] Delegate to Apache Arrow / Gandiva for query execution using LLVM
- [ ] Implement query execution: Sort
- [ ] Implement query execution: Aggregate (min/max/count/sum/avg)
- [ ] Implement query execution: Scalar UDFs
- [ ] Implement query execution: Array UDFs
- [ ] Implement query execution: StructArray (nested objects + dot notation)
- [ ] JOIN support

## 0.8.0 - Distributed queries

- [ ] Distributed query planner
- [ ] Worker can delegate portions of query plan to other workers
- [ ] Data source meta-data

## 0.9.0 - Performance optimizations

- [ ] Query optimizer improvements

## 1.0.0 - Usability

- [ ] Web user interface / better tools / monitoring etc

## Backlog / TBD

- [ ] Support for S3
- [ ] Support for HDFS
- [ ] Authentication/authorization
- [ ] Encryption at rest and in transit
