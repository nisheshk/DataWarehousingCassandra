#Add java JDK version 8 
FROM ubuntu:16.04

#Update the alpine repo and install few packages.

RUN apt-get update -y && apt-get install -y \
        software-properties-common

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

RUN apt install python3-pip -y
RUN pip3 install virtualenv
RUN virtualenv -p python3 venv
ENV VIRTUAL_ENV /venv
ENV PATH /venv/bin:$PATH
RUN virtualenv env
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH 
RUN apt install -y wget tar bash nano

#Download the latest version of spark
RUN wget https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz

#Installing spark is as simple as extracting the file
#Move the extracted folder to root and remove the downloaded file
RUN tar -xzf spark-2.4.6-bin-hadoop2.7.tgz && \
    mv spark-2.4.6-bin-hadoop2.7 /spark && \
    rm spark-2.4.6-bin-hadoop2.7.tgz


#Copy the shell script to the master and worker node
COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh

#Copy cassandra connector
COPY spark-cassandra-connector-2.4.0-s_2.11.jar /spark/jars


