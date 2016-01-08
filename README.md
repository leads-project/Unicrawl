# Unicrawl

Unicrawl is a geo-distributed crawler solution that orchestrates several geographically distributed sites.
Each site operates an independent crawler and relies on well-established techniques for fetching and parsing the content of the web.
Unicrawl splits the crawled domain space across the sites and federates their storage and computing resources, while minimizing thee inter-site communication cost.

Unicrawl is built upon several mature technologies; namely:
- [Apache Nutch](http://nutch.apache.org/) (version 2.x),
- [Apache Gora](http://gora.apache.org/) (version 0.5),
- [Infinispan](infinispan.org) (version 7.x), and
- [Apache Hadoop](https://hadoop.apache.org/) (version 2.5.x).

At core, Unicrawl is a fork of Apache Nutch 2.2.
It offers the very same interface and capabilities as this open source web crawler.
Namely, Unicrawl supports a fine-grained configuration, allows the use of custom plug-ins,
and is a batch-processing oriented software (using the MapReduce support of Apache Hadoop).
In addition, Unicrawl supports geo-replication and page versioning.

Unicrawl was presented at the IEEE CLOUD 2015 conference;
a white paper is available [online](https://drive.google.com/open?id=0BwFkGepvBDQoakFGdkpKNUNCWmM).

The tests available in [InfinispanMultiSiteNutchTest](https://github.com/leads-project/Unicrawl/blob/4fa7107cc4ded4dda08c07dc4a97721397e14949/src/test/java/org/apache/nutch/multisite/InfinispanMultiSiteNutchTest.java) offer a complete view of the capabilities of Unicrawl in terms of seeding, remote pages fetching and update of the crawl database.
These tests also demonstrate in an emulated geo-distributed environment how Unicrawl behaves.

## For the impatient

We explain below how to quickly set-up a solution for a deployment on the local machine.

## Complete Installation

## Usage

## Contact

Any questions can be addressed to [Pierre Sutra](mailto:0track[@]gmail.com).
