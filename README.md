# DevTalks2018 TL&DR
This repository contains the materials used during my presentation **"High performance message brokers out of the IoT world"** at DevTalks 2018 in Cluj.
Content:
- Emqtt broker configured to to manager ~1mil connections per node
- SpringBoot based project for a sample java MqttProducer and MqttConsumer (made using SpringIntegration and Paho)
- Slides used in the talk

# EMQTT - Docker Container
Download EMQ 2.x Docker Image:

`http://emqtt.com/downloads/latest/docker`

Unzip emqttd-docker image:

`unzip emqttd-docker-v2.x.zip`

Load Docker Image:

`docker load < emqttd-docker-v2.x`

Run the Container:

`docker run -tid --name emq20 -p 1883:1883 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqttd-docker-v2.x`

Stop the broker:

`docker stop emq20`

Start the broker:

`docker start emq20`

Enter the running container:

`docker exec -it emq20 /bin/sh`

# SpringBoot code
The samples in the project have been built using Spring Boot 2.x and Spring (Integration) 5.x
- **MqttProducer** - contains the basic configuration for a producer using the Paho mqtt client within a Spring Boot Integration app
- **MqttSubscriber** - same as above for a subscriber
- **MqttDemoConstants** - configurations for the host, QoS, topic from the demo


# Slides
https://docs.google.com/presentation/d/1yL-rsKbpufACk4YNTtDUxt5HGxMetLDs0Jr-ZrOSZRs/edit?usp=sharing
