mvn clean compile package install -DskipTests
sleep 5
bin/start-hbase.sh
sleep 5
bin/local-regionservers.sh start 2 3 
sleep 5
bin/hbase shell
