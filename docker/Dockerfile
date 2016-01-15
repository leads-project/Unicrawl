FROM java:7
# Install maven
RUN wget http://mirror.easyname.ch/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
RUN tar xf apache-maven-3.3.9-bin.tar.gz
RUN rm apache-maven-3.3.9-bin.tar.gz
RUN mv apache-maven-3.3.9 /usr/share/maven
RUN ln -s /usr/share/maven/bin/mvn /usr/local/bin/mvn

# build then launch (Leads-)Infinispan
RUN git clone https://github.com/otrack/Leads-infinispan.git
WORKDIR /Leads-infinispan
RUN mvn clean install -DskipTests -s maven-settings.xml

# build and install Apache Gora
WORKDIR /
RUN git clone https://github.com/otrack/gora
WORKDIR /gora
RUN mvn clean install -DskipTests

# build and package unicrawl
WORKDIR /
RUN git clone https://github.com/vschiavoni/Unicrawl.git
WORKDIR /Unicrawl
RUN mvn clean package -DskipTests -s maven-settings.xml

RUN cp /Unicrawl/src/main/resources/infinispan/clustered.xml /Leads-infinispan/server/integration/build/target/infinispan-server-7.0.1-SNAPSHOT/standalone/configuration/
RUN /Leads-infinispan/server/integration/build/target/infinispan-server-7.0.1-SNAPSHOT/bin/clustered.sh &

# Install Hadoop in Pseudo-distributed mode
WORKDIR /
RUN wget http://mirror.easyname.ch/apache/hadoop/common/hadoop-2.5.2/hadoop-2.5.2.tar.gz
RUN tar xzvf hadoop-2.5.2.tar.gz && rm hadoop-2.5.2.tar.gz
COPY core-site.xml /hadoop-2.5.2/etc/hadoop/
COPY hdfs-site.xml /hadoop-2.5.2/etc/hadoop/
COPY mapred-site.xml /hadoop-2.5.2/etc/hadoop/

#TOFINISH HADOOP

# create a unicrawl deployment
RUN mkdir /tmp/Unicrawl
WORKDIR /tmp/Unicrawl
RUN cp -Rf /Unicrawl/target/lib/ .
RUN cp -Rf /Unicrawl/target/nutch-2.2.jar .
RUN cp -Rf /Unicrawl/src/main/bin .
#RUN # set $HADOOP_DIST_HOME and $NUTCH_DIR in bin/config.sh to right values

COPY config.sh /tmp/Unicrawl/bin/

RUN cp -Rf /Unicrawl/plugin .
RUN mkdir /tmp/Unicrawl/conf
RUN cp /Unicrawl/src/main/resources/regex-urlfilter.txt.template /tmp/Unicrawl/conf
RUN cp /Unicrawl/src/main/resources/regex-normalize.xml.template /tmp/Unicrawl/conf
RUN cp /Unicrawl/src/test/resources/parse-plugins.xml /tmp/Unicrawl/conf
WORKDIR /tmp/Unicrawl/conf
RUN jar uvf ../nutch-2.2.jar *

WORKDIR /tmp/Unicrawl
RUN mkdir inject
RUN echo "http://google.com" >> inject/seed 
RUN sh bin/setup.sh


