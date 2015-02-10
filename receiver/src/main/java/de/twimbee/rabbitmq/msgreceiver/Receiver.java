package de.twimbee.rabbitmq.msgreceiver;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import de.twimbee.rabbitmq.msgproducer.Message;

public class Receiver {

    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, ShutdownSignalException, ConsumerCancelledException,
            InterruptedException {

        // RabbitMQ-Server connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declaring the queue if non-existent (no config needed in rabbitmq-server)
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println("Connected, waiting for messages...");

        QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, queueingConsumer);

        ObjectMapper mapper = new ObjectMapper();

        while (true) {
            Delivery delivery = queueingConsumer.nextDelivery();

            // Read message as String
            String message = new String(delivery.getBody());
            System.out.println("[" + new Date().toString() + "] Received: " + message);

            // It starts with "{", this MUST be JSON!!!!
            if (delivery.getBody()[0] == 123) {
                // We assume this is of type message now
                Message msg = mapper.readValue(delivery.getBody(), Message.class);
                System.out.println("Got message from " + msg.getUsername() + "!");
            }

        }

    }
}
