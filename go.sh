#mvn clean compile package install -DskipTests
#cp /home/leochen4891/github/hbase/hbase-server/target/hbase-server-2.0.0-SNAPSHOT.jar /home/leochen4891/.m2/repository/org/apache/hbase/hbase-server/2.0.0-SNAPSHOT/hbase-server-2.0.0-SNAPSHOT.jar
bin/start-hbase.sh
sleep 5
bin/local-regionservers.sh start 2 3 
sleep 5
bin/hbase shell
