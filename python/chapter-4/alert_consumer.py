###############################################
# RabbitMQ in Action
# Chapter 4.2.2 - Alerting Server Consumer
# 
# Requires: pika >= 0.9.5
# 
# Author: Jason J. W. Williams
# (C)2011
###############################################
import json, smtplib
import pika


def send_mail(recipients, subject, message):
    """E-mail generator for received alerts."""
    return
    headers = ("From: %s\r\nTo: \r\nDate: \r\n" + \
               "Subject: %s\r\n\r\n") % ("alerts@ourcompany.com",
                                         subject)
    
    smtp_server = smtplib.SMTP()
    smtp_server.connect("mail.ourcompany.com", 25)
    smtp_server.sendmail("alerts@ourcompany.com",
                         recipients,
                         headers + str(message))
    smtp_server.close()

# python alert_producer.py -m adsf -r critical.rate_limit
#
# delivery_tag 15
# consumer_tag rate_limit
# Sent alert via e-mail! Alert Text: adsf  Recipients: ['api.team@ourcompany.com']
# delivery_tag 16
# consumer_tag critical
# Sent alert via e-mail! Alert Text: adsf  Recipients: ['ops.team@ourcompany.com']

#/(asc.5) Notify Processors
def critical_notify(channel, method, header, body):
    """Sends CRITICAL alerts to administrators via e-mail."""
    print 'delivery_tag', method.delivery_tag
    print 'consumer_tag', method.consumer_tag

    EMAIL_RECIPS = ["ops.team@ourcompany.com",]
    
    #/(asc.6) Decode our message from JSON    
    message = json.loads(body)

    #/(asc.7) Transmit e-mail to SMTP server
    send_mail(EMAIL_RECIPS, "CRITICAL ALERT", message)
    print ("Sent alert via e-mail! Alert Text: %s  " + \
           "Recipients: %s") % (str(message), str(EMAIL_RECIPS))
    
    #/(asc.8) Acknowledge the message
    channel.basic_ack(delivery_tag=method.delivery_tag)

def rate_limit_notify(channel, method, header, body):
    """Sends the message to the administrators via e-mail."""
    print 'delivery_tag', method.delivery_tag
    print 'consumer_tag', method.consumer_tag

    EMAIL_RECIPS = ["api.team@ourcompany.com",]
    
    #/(asc.9) Decode our message from JSON
    message = json.loads(body)
    
    #/(asc.10) Transmit e-mail to SMTP server
    send_mail(EMAIL_RECIPS, "RATE LIMIT ALERT!", message)

    print ("Sent alert via e-mail! Alert Text: %s  " + \
           "Recipients: %s") % (str(message), str(EMAIL_RECIPS))
    
    #/(asc.11) Acknowledge the message
    channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    #/(asc.0) Broker settings
    AMQP_SERVER = "localhost"
    AMQP_USER = "alert_user"
    AMQP_PASS = "alertme"
    AMQP_VHOST = "/"
    AMQP_EXCHANGE = "alerts"
    
    #/(asc.1) Establish connection to broker
    creds_broker = pika.PlainCredentials(AMQP_USER, AMQP_PASS)
    conn_params = pika.ConnectionParameters(AMQP_SERVER,
                                            virtual_host = AMQP_VHOST,
                                            credentials = creds_broker)
    conn_broker = pika.BlockingConnection(conn_params)
    
    channel = conn_broker.channel()
    
    #/(asc.2) Declare the Exchange
    channel.exchange_declare( exchange=AMQP_EXCHANGE,
                              exchange_type="topic",
                              auto_delete=False)
    
    #/(asc.3) Build the queues and bindings for our topics    
    channel.queue_declare(queue="critical", auto_delete=False)
    channel.queue_bind(queue="critical",
                       exchange="alerts",
                       routing_key="critical.*")
    
    channel.queue_declare(queue="rate_limit", auto_delete=False)
    channel.queue_bind(queue="rate_limit",
                       exchange="alerts",
                       routing_key="*.rate_limit")
    
    #/(asc.4) Make our alert processors
    
    channel.basic_consume( "critical",
                           critical_notify,
                           auto_ack=False,
                           consumer_tag="critical")
    
    channel.basic_consume( "rate_limit",
                           rate_limit_notify,
                           auto_ack=False,
                           consumer_tag="rate_limit")
    
    print "Ready for alerts!"
    channel.start_consuming()
    