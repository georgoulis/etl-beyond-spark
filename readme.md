# ETL Beyond Spark – Benchmarking Project

This repository contains the code and experiments behind the talk “ETL Beyond Spark” (PyCon 20XX).
We benchmark Polars, DuckDB, and Apache Spark on ETL-style workloads, focusing on cost efficiency (GB per Dollar) across different data scales.

# 📊 Benchmark Summary
Scale Factor	Polars	DuckDB	Spark
sf10	151.65	102.73	0.91
sf50	279.45	241.17	8.11
sf100	250.25	144.74	13.71
sf500	200.43	182.41	54.85

(GB processed per Dollar spent)

# 🛠️ Tools Compared

Polars
: A lightning-fast DataFrame library built in Rust.

DuckDB
: The “SQLite of Analytics”, an embedded OLAP SQL engine.

Apache Spark
: A distributed processing framework for large-scale data.

# ⚠️ Notes & Caveats

These benchmarks are exploratory and not production-optimized.

Costs and performance depend on hardware, configs, and data distributions.

Goal: Show trade-offs and spark discussion, not declare a “winner”.

# 📌 Next Steps

Deeper optimization passes on all three engines.

Experiment with hybrid / multi-engine ETL setups.

Publish updated benchmarks in this repo.