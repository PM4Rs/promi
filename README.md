# promi
**Process Mining for Rust**

[![Build Status](https://travis-ci.org/PM4Rs/promi.svg?branch=master)](https://travis-ci.org/PM4Rs/promi)
[![Crate](https://img.shields.io/crates/v/promi)](https://crates.io/crates/promi)
[![API](https://docs.rs/promi/badge.svg)](https://docs.rs/promi)
[![License](https://img.shields.io/crates/l/promi)](https://crates.io/crates/promi#license)
[![Downloads](https://img.shields.io/crates/d/promi)](https://crates.io/crates/promi)

## Roadmap `0.x.x`
* **`0.0.x` get in shape**
    * harmonize imports
    * clean & refactor error handling
    * add missing documentation
    * finalize `Cargo.toml`
    * setup github
        * organization
        * issue templates / RFC
        * translate roadmap to github issues and milestones
* **`0.1.x` stabilize basic streaming**
    * Classifier/Global: make scope attribute optional
    * `Buffer`
        * add access control & thread safety
        * signal whether the stream stopped or pending
    * `Filter`
        * implementation as `Stream` and/or `Handler`
        * accept arbitrary number of functions that map events/traces to boolean value (logical disjunction)
        * logical conjuction is supported via filter chaining
    * `Stats`
        * event / trace specific statistics
        * aggregation methods (mean, median etc.)
    * implement `XesValidator`
    * implement `Observer`
    * implement `XesExtension`
        * registry of default extensions
    * implement `XesRescue`
    * serde: binary XES
    * convenience functions
        * &str -> Log:      &str | BufReader > XesReader > Log
        * path -> Log:      path | file > XesReader > Log
        * Log -> String:    Log | Buffer > XesWriter > ?
        * Log -> File:      Log | Buffer > XesWriter > File
    * add integration tests / examples covering current features
* **`0.2.x` optimization**
    * benchmarks (e.g. against PM4Py or OpenXES)
    * reduce memory footprint, unnecessary copies (--> Observer <--), concurrency
* **`0.3.x` mining**
    * frequent item sets, association rules (FP-growth)
    * process mining
        * footprint / directly follows graph generation
        * alpha/heuristic/inductive miner
        * token replay / conformance checking
    * visualization
* **`0.4.x` promotion**
    * blog post: something like "promi: Process Mining by example"


## Nice to have
* application logging (`log` trait) to event stream
* trace factory: buffer events and aggregate them to traces
* random log / event stream factory
* declarative (e.g. yaml based) pipeline building
* more exotic targets such as embedded devices and web assembly
* advanced testing: compatibility of promi XES with _OpenXES_, _PM4Py_ etc.
    * check against schema definition e.g. using `xmllint`
    
## License
Copyright © 2020 The _promi_ Developers

_promi_ is licensed under MIT **OR** Apache 2.0 license
