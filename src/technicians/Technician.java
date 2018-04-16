package technicians;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class Technician {

    private static String type1 = "elbow";
    private static String type2 = "hip";
    private static Channel channel;
    private static String EXCHANGE_NAME = "X1";
    private static String FANOUT_EXCHANGE_NAME = "X2";
    private static String adminQueueName;

    public static void main(String[] args) throws IOException, TimeoutException {
        connectToChannel();
        declareExchange();
        setupConsumption();
        System.out.println("I am technician who examines " + type1 + " and " + type2);
    }

    private static void setupConsumption() throws IOException {
        channel.basicQos(1);
        //First Type
        String queueName1 = declareQueue(type1);
        String queueName2 = declareQueue(type2);
        adminQueueName = channel.queueDeclare().getQueue();
        channel.queueBind(adminQueueName, FANOUT_EXCHANGE_NAME, "");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Examination: " + message + " is done.");
                channel.basicPublish(EXCHANGE_NAME, properties.getReplyTo(), null, (message + "[DONE]").getBytes("UTF-8"));
            }
        };

        System.out.println("Waiting for messages...");
        channel.basicConsume(queueName1, true, consumer);
        channel.basicConsume(queueName2, true, consumer);
        channel.basicConsume(adminQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                System.out.println("Admin: " + new String(body));
            }
        });
    }

    private static String declareQueue(String type) throws IOException {
        String queueName = channel.queueDeclare(type, false, false, false, null).getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, type);
        System.out.println("Created queue: " + queueName);
        return queueName;
    }


    private static void declareExchange() throws IOException {
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        System.out.println("Declared exchange");
    }

    private static void connectToChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        channel  = factory.newConnection().createChannel();
        System.out.println("Connection to localhost successful");
    }
}
