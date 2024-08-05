import pika
import json
from models import Contact
from faker import Faker

fake = Faker()


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


channel.queue_declare(queue='email_queue')
channel.queue_declare(queue='sms_queue')


for _ in range(10): 
    contact = Contact(
        fullname=fake.name(),
        email=fake.email(),
        phone_number=fake.phone_number(),
        preferred_contact_method=fake.random.choice(['email', 'sms'])
    )
    contact.save()

    message = {
        'contact_id': str(contact.id),
        'preferred_contact_method': contact.preferred_contact_method
    }

    if contact.preferred_contact_method == 'email':
        channel.basic_publish(exchange='', routing_key='email_queue', body=json.dumps(message))
    else:
        channel.basic_publish(exchange='', routing_key='sms_queue', body=json.dumps(message))

connection.close()
