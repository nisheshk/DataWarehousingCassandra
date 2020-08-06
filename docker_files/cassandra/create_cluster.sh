#remove all the containers
sudo docker kill $(sudo docker ps -a | grep cassandra | awk {'print$1'})
sudo docker rm $(sudo docker ps -a | grep cassandra | awk {'print$1'})

echo 'Creating first cassandra node'
#Create a single instance
sudo docker run --name my-cassandra-1 -m 1g -d cassandra:latest
echo 'Creation of first cassandra node successful'
sleep 5

#Find out the IP address of first node of cassandra
sudo docker inspect --format='{{ .NetworkSettings.IPAddress }}' my-cassandra-1

#Run cqlsh
sudo docker run -it --link my-cassandra-1 --rm cassandra:latest bash -c 'exec cqlsh 172.17.0.2'

#Create second node and create a link to the first node
sudo docker run --name my-cassandra-2 -m 1g -d \
-e CASSANDRA_SEEDS="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' my-cassandra-1)" \
cassandra:latest
sudo docker inspect --format='{{ .NetworkSettings.IPAddress }}' my-cassandra-2





