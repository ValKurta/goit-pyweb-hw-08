from mongoengine import Document, StringField, ReferenceField, ListField, BooleanField, connect


connect('Cluster0', host='mongodb+srv://valentynkurta:P2BAxxTRU!2A2U@cluster0.9zkgeju.mongodb.net/Cluster0?retryWrites=true&w=majority')


class Author(Document):
    fullname = StringField(required=True)
    born_date = StringField()
    born_location = StringField()
    description = StringField()


class Quote(Document):
    tags = ListField(StringField())
    author = ReferenceField(Author)
    quote = StringField(required=True)


class Contact(Document):
    fullname = StringField(required=True)
    email = StringField(required=True)
    phone_number = StringField()
    preferred_contact_method = StringField(choices=['email', 'sms'], default='email')
    message_sent = BooleanField(default=False)
