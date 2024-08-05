import pika
import json
from models import Contact


def send_sms(contact):
    print(f"Sending SMS to {contact.phone_number}")
    contact.message_sent = True
    contact.save()


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


channel.queue_declare(queue='sms_queue')


def callback(ch, method, properties, body):
    message = json.loads(body)
    contact = Contact.objects(id=message['contact_id']).first()
    if contact:
        send_sms(contact)


channel.basic_consume(queue='sms_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
