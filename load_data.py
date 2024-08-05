import json
import os
from models import Author, Quote


authors_file = 'authors.json'
quotes_file = 'quotes.json'

if not os.path.exists(authors_file):
    raise FileNotFoundError(f"there is no {authors_file}")
if not os.path.exists(quotes_file):
    raise FileNotFoundError(f"There is no {quotes_file}")


with open(authors_file, 'r', encoding='utf-8') as f:
    authors_data = json.load(f)
    for item in authors_data:
        author = Author(
            fullname=item['fullname'],
            born_date=item['born_date'],
            born_location=item['born_location'],
            description=item['description']
        )
        author.save()


with open(quotes_file, 'r', encoding='utf-8') as f:
    quotes_data = json.load(f)
    for item in quotes_data:
        author = Author.objects(fullname=item['author']).first()
        if author:
            quote = Quote(
                tags=item['tags'],
                author=author,
                quote=item['quote']
            )
            quote.save()
        else:
            print(f"Author {item['author']} is not found in db")
