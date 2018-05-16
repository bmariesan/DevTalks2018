package ro.bmariesan.devtalksmqtt;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static ro.bmariesan.devtalksmqtt.MqttDemoConstants.*;

@SpringBootApplication
@IntegrationComponentScan
@EnableAutoConfiguration
@Component
public class MqttProducer {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(MqttProducer.class)
                .web(WebApplicationType.NONE)
                .run(args);
        MyGateway gateway = context.getBean(MyGateway.class);
        MqttProducer producer = new MqttProducer();
        producer.sendMessages(gateway);
    }

    private void sendMessages(MyGateway gateway) {
        for (int i = 1; i <= 100; i++) {
            gateway.sendToMqtt("Hello DevTalks!");
        }
    }

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(mqttConnectOptions());
        factory.setPersistence(mqttClientDataStore());
        return factory;
    }

    @Bean
    public MqttConnectOptions mqttConnectOptions() {
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setServerURIs(new String[]{"tcp://localhost:1883"});
        mqttConnectOptions.setWill(DEV_TALKS_TOPIC, "I'll be back...".getBytes(), DEFAULT_QOS, true);
        mqttConnectOptions.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
        return mqttConnectOptions;
    }

    @Bean
    public MemoryPersistence mqttClientDataStore() {
        return new MemoryPersistence();
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttOutboundChannel")
    public MessageHandler mqttOutbound() {
        String clientId = UUID.randomUUID().toString();
        MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler(clientId, mqttClientFactory());
        messageHandler.setDefaultTopic(DEV_TALKS_TOPIC);
        messageHandler.setDefaultQos(DEFAULT_QOS);
        messageHandler.setCompletionTimeout(COMPLETION_TIMEOUT);
        messageHandler.start();
        return messageHandler;
    }

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @MessagingGateway(defaultRequestChannel = "mqttOutboundChannel")
    private interface MyGateway {
        void sendToMqtt(String data);
    }
}