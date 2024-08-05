import redis
from models import Quote, Author
from mongoengine import connect


connect('Cluster0',
        host='mongodb+srv://valentynkurta:P2BAxxTRU!2A2U@cluster0.9zkgeju.mongodb.net/Cluster0?retryWrites=true&w=majority')


cache = redis.Redis(host='localhost', port=6379, db=0)


def search_by_name(name):
    key = f"name:{name.lower()}"
    cached_result = cache.get(key)
    if cached_result:
        return cached_result.decode('utf-8')

    author = Author.objects(fullname__icontains=name).first()
    if not author:
        return f"No author found with name {name}"

    quotes = Quote.objects(author=author)
    result = '\n'.join([quote.quote for quote in quotes])
    cache.set(key, result)
    return result


def search_by_tag(tag):
    key = f"tag:{tag.lower()}"
    cached_result = cache.get(key)
    if cached_result:
        return cached_result.decode('utf-8')

    quotes = Quote.objects(tags__icontains=tag)
    result = '\n'.join([quote.quote for quote in quotes])
    cache.set(key, result)
    return result


def search_by_tags(tags):
    results = []
    for tag in tags:
        results.append(search_by_tag(tag))
    return '\n'.join(results)


while True:
    command = input("Enter command: ").strip()
    if command.lower() == 'exit':
        break
    if command.startswith('name:'):
        name = command.split(':')[1].strip()
        print(search_by_name(name))
    elif command.startswith('tag:'):
        tag = command.split(':')[1].strip()
        print(search_by_tag(tag))
    elif command.startswith('tags:'):
        tags = command.split(':')[1].strip().split(',')
        print(search_by_tags(tags))
