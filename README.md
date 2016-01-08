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

Unicrawl was developped in the context of the EU FP7 LEADS project whose goal is build a cloud service platform for Big Data as a Service.
More details on the LEADS project are available on the [following](http://www.leads-project.eu) web site.

## Installation

A complete deployment of Unicrawl requires to install an Infinispan cluster
and on the very same machine, an Hadoop cluster.
Once these two distributed systems are deployed and operational,
Unicrawl is executed as a regular MapReduce job atop of Hadoop.
Infinispan is used as the backing store of the web crawler.

Below, we explain how to quickly run Unicrawl on your local machine.

```
# build then launch (Leads-)Infinispan
git clone https://github.com/leads-project/Leads-infinispan
cd Leads-infinispan
export ISPN_DIR=`pwd`
mvn clean install -DskipTests

# build and install Apache Gora
git clone https://github.com/leads-project/gora
cd gora
export GORA_DIR=`pwd`
mvn clean install -DskipTests

# build and package unicrawl
git clone https://github.com/leads-project/Unicrawl
export UNICRAWL_DIR=`pwd`
mvn clean package -DskipTests

# launch infinispan
# set right configuration
cp ${UNICRAWL_DIR}/src/main/resources/infinispan/clustered.xml ${ISPN_DIR}/server/integration/build/target/infinispan-server-7.0.1-SNAPSHOT/standalone/configuration/
# start-up the server
${ISPN_DIR}/server/integration/build/target/infinispan-server-7.0.1-SNAPSHOT/bin/clustered.sh &

# install and launch Apache Hadoop following the
# [official](https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html) tutorial,
# in pseudo-distributed operation mode

# create a unicrawl deployment
mkdir /tmp/Unicrawl
cd /tmp/Unicrawl
cp -Rf ${UNICRAWL_DIR}/target/lib/ .
cp -Rf ${UNICRAWL_DIR}/target/lib/nutch-2.2.jar .
cp -Rf ${UNICRAWL_DIR}/src/main/bin .
# set $HADOOP_DIST_HOME and $NUTCH_DIR in bin/config.sh to right values
cp -Rf ${UNICRAWL_DIR}/plugin .
mkdir conf
cp ${UNICRAWL_DIR}/src/main/resources/src/regex-urlfilter.xml.template ./conf/regex-urlfilter.xml
cp ${UNICRAWL_DIR}/src/main/resources/src/regex-normalize.xml.template ./conf/regex-normalize.xml
cd conf
jar uvf ../lib/nutch-2.2.jar *

# create seed and set-up unicrawl
mkdir inject
echo "http://google.com" >> inject/seed
./bin/setup.sh

# insert seed and start one round of crawl
./bin/dnutch --seed
./bin/dnutch 1

# list the information in the (infinispan) crawl database
# without the page content itself
rm -Rf dump && ./bin/nutch readdb -dump dump

```

## Contact

Any questions can be addressed to [Pierre Sutra](https://sites.google.com/site/0track).
