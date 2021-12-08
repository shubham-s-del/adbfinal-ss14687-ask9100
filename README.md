# adbfinal

Contributors:

Name: Shubham Srivastava

NetID: ss14687

Name: Adit Kotwal

NetID: ask9100



#Running using reprozip
1. mvn clean compile assembly:single
2. ../reprozip trace java -jar target/adbfinal-1.0-SNAPSHOT-jar-with-dependencies.jar src/main/resources/input/input-1
3. ./reprozip pack adbms_project_file
4. Create directory within project folder - Testing
5. ./reprounzip directory setup project_file.rpz ./testing
6. java -jar target/adbfinal-1.0-SNAPSHOT-jar-with-dependencies.jar src/main/resources/input/input-1

Note that input-1 can be replaced by any input file name. 