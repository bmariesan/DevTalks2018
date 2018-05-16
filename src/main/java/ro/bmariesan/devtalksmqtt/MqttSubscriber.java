package ro.bmariesan.devtalksmqtt;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static ro.bmariesan.devtalksmqtt.MqttDemoConstants.*;

@SpringBootApplication
@IntegrationComponentScan
@EnableAutoConfiguration
@Component
public class MqttSubscriber {

    private long numMessages = 100;
    private long count = 0;

    public static void main(String[] args) {
        new SpringApplicationBuilder(MqttSubscriber.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(mqttConnectOptions());
        return factory;
    }

    @Bean
    public MqttConnectOptions mqttConnectOptions() {
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setServerURIs(new String[]{"tcp://localhost:1883"});
        mqttConnectOptions.setWill(DEV_TALKS_TOPIC, "I'll be back again...".getBytes(), DEFAULT_QOS, true);
        mqttConnectOptions.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
        return mqttConnectOptions;
    }

    @Bean
    public MessageProducer mqttInbound() {
        String clientId = UUID.randomUUID().toString();

        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), DEV_TALKS_TOPIC);

        adapter.setCompletionTimeout(COMPLETION_TIMEOUT);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(DEFAULT_QOS);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        return message -> {
            if (count % 10 == 0) {
                System.out.println(String.format("Received %d numMessages from " + DEV_TALKS_TOPIC + " topic.", count));
            }
            if (count == numMessages) {
                System.exit(0);
            }
            System.out.println(message.getPayload());
            count++;
        };
    }
}