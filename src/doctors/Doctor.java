package doctors;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class Doctor {
    private static final String EXCHANGE_NAME = "X1";
    private static String FANOUT_EXCHANGE_NAME = "X2";
    private static Channel channel;
    private static String responseQueueName;
    private static String adminQueueName;

    public static void main(String[] args) throws IOException, TimeoutException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        connectToChannel();
        declareExchange();
        bindResponseQueue();
        while (true) {
            System.out.println("Enter patient name: ");
            String patient = reader.readLine();
            System.out.println("Enter examination name:(HIP, KNEE, ELBOW)");
            String examination = reader.readLine();
            if (patient.equals("exit")) break;
            switch (examination.toLowerCase()) {
                case "hip":
                case "knee":
                case "elbow": {
                    publishMessage(patient, examination);
                    break;
                }
                default:
                    System.out.println("Improper examination name");
                    break;
            }

        }
    }


    private static void bindResponseQueue() throws IOException {
        responseQueueName = channel.queueDeclare().getQueue();
        adminQueueName = channel.queueDeclare().getQueue();
        channel.queueBind(responseQueueName, EXCHANGE_NAME, responseQueueName);
        channel.queueBind(adminQueueName, FANOUT_EXCHANGE_NAME, "");
        System.out.println("ResponseQueue created: " + responseQueueName);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message);
            }
        };
        channel.basicConsume(responseQueueName, true, consumer);
        channel.basicConsume(adminQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                System.out.println("Admin: " + new String(body));
            }
        });
    }

    private static void publishMessage(String patient, String examination) throws IOException {
        String message = "(" + patient + "," + examination + ")";
        System.out.println("Publishing: " + message + " with response to: " + responseQueueName);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().replyTo(responseQueueName).build();
        channel.basicPublish(EXCHANGE_NAME, examination.toLowerCase(), properties, message.getBytes("UTF-8"));
    }

    private static void declareExchange() throws IOException {
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        System.out.println("Declared exchange");
    }

    private static void connectToChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        System.out.println("Connection to localhost successful");
    }
}
