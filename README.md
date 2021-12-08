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


#Running using sourcecode
1. Download code and cd into the repo (adbfinal-ss14687-ask9100)
2. mvn clean compile assembly:single
3. java -jar target/adbfinal-1.0-SNAPSHOT-jar-with-dependencies.jar src/main/resources/input/input-1

Specify the input file as required. 

Please contact us for help while running.