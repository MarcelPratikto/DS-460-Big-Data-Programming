# Taking the plunge with Polars

# Slides

The [HTML Slides](https://quickskilling.github.io/polars_guide/) and [pdf slides](https://github.com/quickskilling/polars_guide/blob/slides/slides.pdf) for this skill are available.

## What is Polars?

> The goal of `Polars` is to provide a lightning fast `DataFrame` library that:

> - Utilizes all available cores on your machine.
> - Optimizes queries to reduce unneeded work/memory allocations.
> - Handles datasets much larger than your available RAM.
> - Has an API that is consistent and predictable.
> - Has a strict schema (data-types should be known before running the query).
> 
> Polars is written in Rust which gives it C/C++ performance and allows it to fully control performance critical parts in the query engine.
> 
> As such `Polars` goes to great lengths to:
> 
> - Reduce redundant copies.
> - Traverse memory cache efficiently.
> - Minimize contention in parallelism.
> - Process data in chunks.
> - Reuse memory allocations.
> 
> [Polars Documentation](https://pola-rs.github.io/polars-book/user-guide/)

Their documenation compares Polars to some other Python data science tools

> **Pandas**

> A very versatile tool for small data. [Read 10 things I hate about pandas](https://wesmckinney.com/blog/apache-arrow-pandas-internals/) written by the author himself. Polars has solved all those 10 things. Polars is a versatile tool for small and large data with a more predictable API, less ambiguous and stricter API.

> The API of pandas was designed for in memory data. This makes it a poor fit for performant analysis on large data (read anything that does not fit into RAM). Any tool that tries to distribute that API will likely have a suboptimal query plan compared to plans that follow from a declarative API like SQL or polars' API.

> **DuckDB**

> Polars and DuckDB have many similarities. DuckDB is focussed on providing an in-process OLAP Sqlite alternative, polars is focussed on providing a scalable DataFrame interface to many languages. Those different front-ends lead to different optimization strategies and different algorithm prioritization. The interop between both is zero-copy. See more: https://duckdb.org/docs/guides/python/polars

> **Spark**

Spark is designed for distributed workloads and uses the JVM. The setup for spark is complicated and the startup-time is slow. On a single machine Polars has much better performance characteristics. If you need to process TB's of data spark is a better choice.

> [Polars Documentation](https://pola-rs.github.io/polars-book/user-guide/)


### Background

#### Why Rust?

The top benefit of Rust coding is its adept memory management. The benefits of programing in Rust don’t stop at memory management. It’s fast and reliable for creating cross-platform applications, and it can integrate with preexisting code. The Rust programming language is well-suited for projects that demand extremely high performance. Its ability to process large amounts of data and CPU-intensive operations makes it a strong competitor in the data science space space. [Adapted from Valerie Silverthorne ·
Jul 21, 2020](https://about.gitlab.com/blog/2020/07/21/rust-programming-language/)


