# Log Reader Solution

```
Usage of ./ciqth:
  -analyze
    	analyze dataset after import (answer the example stakeholder questions)
  -skipdupes
    	skip duplicate rows rather than erroring on them
```

To build this, you'll need Go (tested on 1.19) and SQLite3 installed (and CGO
enabled). I did not end up with the time to provide a Docker image or Nix
derivation to help here, sorry. A basic unit test for the isolated
string-to-struct row parser is provided, and the requested questions are
answered at runtime by passing `-analyze`, the output of which can serve as
another, crude, form of correctness check (by hand-counting the relevant rows
from the CSV, for now).

The code itself is commented extensively, largely with stream-of-consciousness
thoughts as I went along. I won't claim this to be the most idiomatic Go code in
the world after several years away from the language (spending that time mostly
in TypeScript, Rust, and Ruby+Sorbet), but in general, I tried to avoid
unnecessary allocations, and also anything that would be too horribly unreadable
or hard to understand.

## Data Storage Engines and thoughts thereon

For this solution I went with a SQLite database because it's a great
general-purpose and extremely battle-tested embedded database. However, this was
done as a bit of a compromise based on the questions needing answered on the
output side, moreso than because a relational database is the most optimal data
store for the data as formatted (it probably isn't). The data lends itself best
to a time series database, of which an excellent embedded example [exists for
Go](https://github.com/nakabonne/tstorage): given a time and a value (the size
of the payload), and given attached labels of username and direction, all sorts
of questions about data within a date/time range become straightforward to
answer (in other words, "something like Prometheus but without [the label
cardinality
problems](https://grafana.com/blog/2022/02/15/what-are-cardinality-spikes-and-why-do-they-matter/)"
would be great here). However, the questions at hand are *not* time-aligned in
2/3 of cases, and TSDBs are not generally optimized for searching by labels
first.

In a real-world scenario, either a combination of a TSDB-based thing (maybe
that's Prometheus, maybe that's something else) and a relational DB could be
appropriate, or perhaps I'd instead take the opportunity to evaluate
[TimescaleDB](https://www.timescale.com/) which is quite literally the fusion of
PostgreSQL and efficient time-based indexing.
