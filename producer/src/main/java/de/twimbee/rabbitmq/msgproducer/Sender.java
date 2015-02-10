package de.twimbee.rabbitmq.msgproducer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Sender {

    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException {

        // RabbitMQ-Server connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declaring the queue if non-existent (no config needed in rabbitmq-server)
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        // Send simple String
        String message = "Hello World!";

        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println("Message sent!");

        // Send some Java bbject as JSON
        Message msg = new Message();
        msg.setUsername("Dr4K4n");
        msg.setMessage(message);
        msg.setDate(new Date());

        ObjectMapper mapper = new ObjectMapper();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, msg);

        channel.basicPublish("", QUEUE_NAME, null, out.toByteArray());
        System.out.println("JSON-Message sent!");

        channel.close();
        connection.close();

    }
}
