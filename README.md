# BD24_Project_A6_B



## Getting started
* install kafka
* setup environment
* goto kafka/config/server 
  * uncomment the line "listeners=PLAINTEXT://:9092"
  
* cd kafka in terminal, for starting zookeeper
  *  .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

* open another terminal tab, the path should be c:kafka also here
  *  .\bin\windows\kafka-server-start.bat .\config\server.properties
  
* now both kafka and zookeeper should be running, in new terminal tab, now create a topic by the following command
  * .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic my_topic

* git clone the project
* create virtual environment 
* install the required packages
* run the project

#####DOCKERS##
* Run Dockers app
* run command : docker-compose up --build

 if this is the error: Error response from daemon: Ports are not available: exposing port TCP 0.0.0.0:2181 -> 0.0.0.0:0: listen tcp 0.0.0.0:2181: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.
Then: 
*run: netstat -ano | findstr :2181
run: taskkill /PID portnumber /F
run: netstat -ano | findstr :2181
docker-compose down
docker system prune -a --volumes
docker-compose up -d --build


