#!/usr/bash

java -Dcassandra-foreground  -Dcassandra.config=file:///etc/cassandra/cassandra.yaml  -ea -Xmx4G -classpath target/*:target/dependency/* com.mirantis.magnetodb.sstablegen.Runner default_tenant bigdata file_sha2 file_md5 table.def data.txt 20
