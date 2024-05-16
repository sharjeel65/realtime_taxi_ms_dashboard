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