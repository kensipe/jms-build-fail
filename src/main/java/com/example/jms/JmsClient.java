package com.example.jms;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import java.io.Serializable;

public class JmsClient {
	public static final int DEFAULT_RECEIVE_TIMEOUT = -1;

    private final Logger log = LoggerFactory.getLogger(getClass());
    
	private ConnectionFactory connectionFactory;
    private Destination destination;
    private long receiveTimeout;
    

    public Serializable exchangeObjMsgOverTempQueue(final Serializable requestData) {
        return exchangeObjMsgOverTempQueue(requestData, null, null);
    }

    public Serializable exchangeObjMsgOverTempQueue(final Serializable requestData, final String msgPropertyKey, final String msgPropertyValue) {
        Connection connection = null;
        Session session = null;
        Message response;
        Object responseData = null;
        MessageProducer producer = null;
        MessageConsumer consumer = null;
        Destination temporaryQueue = null;

        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            temporaryQueue = session.createTemporaryQueue();

            final ObjectMessage request = session.createObjectMessage(requestData);
            request.setJMSReplyTo(temporaryQueue);
            if (StringUtils.isNotBlank(msgPropertyKey)) {
                request.setStringProperty(msgPropertyKey, msgPropertyValue);
            }

            producer = session.createProducer(destination);
            consumer = session.createConsumer(temporaryQueue);
            producer.send(request);

            log.debug("JMS data sent {}", request);
            log.debug("JMS data sent payload = key={}, value={}", msgPropertyKey, msgPropertyValue);

            connection.start();
            response = consumer.receive(getReceiveTimeout());
            connection.stop();

            if (response != null && (response instanceof ObjectMessage)) {
                responseData = ((ObjectMessage) response).getObject();
            }

            log.debug("JMS data recieved {}", response);

            if (responseData != null && responseData instanceof ObjectMessage) {
                final String objectString = ObjectUtils.toString(((ObjectMessage) responseData).getObjectProperty("Message"));
                log.debug("JMS data recieved payload = {}", objectString);
            }

            log.info("Exchanged message over temporary queue successfully");
        } catch (final JMSException ex) {
            log.error("Trying to send and recieve JMS message.", ex);
            throw new RuntimeException("Could not send and receive JMS " + "message over temporary queue", ex);
        } finally {

            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (final Exception ex) {
                log.error("Exception while closing the consumer : " + ex);
            }
            try {
                if (temporaryQueue != null) {
                    ((TemporaryQueue) temporaryQueue).delete();
                }
            } catch (final Exception ex) {
                log.error("Exception while trying to delete Temporary Queue : " + ex);
            }
            try {
                if (session != null) {
                    session.close();
                }
            } catch (final JMSException ex) {
                log.error("Trying to close JMS Session.", ex);
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (final JMSException ex) {
                log.error("Trying to close JMS Connection.", ex);
            }


        }
        return (Serializable) responseData;
    }

    // *******************************************
    // ******** Spring injected properties *******
    // *******************************************
    public void setDestination(final Destination destination) {
        this.destination = destination;
    }

    Destination getDestination() {
        return this.destination;
    }

    public void setConnectionFactory(final ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    ConnectionFactory getConnectionFactory() {
        return this.connectionFactory;
    }


    public long getReceiveTimeout() {
        return receiveTimeout;
    }

    public void setReceiveTimeout(final long receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }
}