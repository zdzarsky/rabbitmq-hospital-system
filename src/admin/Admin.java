package admin;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class Admin {
    private static final String EXCHANGE_NAME = "X1";
    private static String FANOUT_EXCHANGE_NAME = "X2";

    private static Channel channel;
    private static String queueName;

    public static void main(String[] args) throws IOException, TimeoutException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        connectToChannel();
        declareQueue();
        declareExchange();
        setupConsumption();
        while (true) {
            String message = reader.readLine();
            if (message.toLowerCase().equals("exit")) break;
            channel.basicPublish(FANOUT_EXCHANGE_NAME, "",null, message.getBytes("UTF-8") );
        }
    }

    private static void declareQueue() throws IOException {
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "#");
        System.out.println("Created queue: " + queueName);
    }

    private static void setupConsumption() throws IOException {
        channel.basicConsume(queueName, true, new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                System.out.println(new String(body));
            }
        });
    }

    private static void declareExchange() throws IOException {
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(FANOUT_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        System.out.println("Declared exchange");
    }

    private static void connectToChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        channel = factory.newConnection().createChannel();
        System.out.println("Connection to localhost successful");
    }
}
