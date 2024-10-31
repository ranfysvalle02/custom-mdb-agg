# example_usage.py

import logging
from custom_mdb_agg.aggregator import CustomMongoAggregator
from custom_mdb_agg.operators import prompt_operator

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize the CustomMongoAggregator class
mongo_db = CustomMongoAggregator(
    uri="mongodb://localhost:27017/?directConnection=true",
    database="mydatabase",
    collection="mycollection"
)
mongo_db.client["mydatabase"]["mycollection"].delete_many({})
DATASET = [
    {
        "_id": 1,
        "user": "Alice",
        "movie": "Inception",
        "rating": 5,
        "comment": "This movie is absolutely amazing! The plot twists and turns in ways you would never expect. Truly a masterpiece.",
        "status": "active"
    },
    {
        "_id": 2,
        "user": "Bob",
        "movie": "The Matrix",
        "rating": 4,
        "comment": "The Matrix offers great visuals. The special effects are truly groundbreaking, making it a visual feast.",
        "status": "active"
    },
    {
        "_id": 3,
        "user": "Charlie",
        "movie": "Interstellar",
        "rating": 5,
        "comment": "Interstellar is a mind-blowing experience! The scientific concepts are intriguing and the storyline is deeply moving.",
        "status": "inactive"
    },
    {
        "_id": 4,
        "user": "Diana",
        "movie": "Inception",
        "rating": 4,
        "comment": "Inception is a good movie, but it can be quite confusing. The plot is complex and requires your full attention.",
        "status": "active"
    },
    {
        "_id": 5,
        "user": "Eve",
        "movie": "The Matrix Reloaded",
        "rating": 2,
        "comment": "The Matrix Reloaded didn't quite live up to the first one. It lacked the originality and depth of its predecessor.",
        "status": "inactive"
    },
    {
        "_id": 6,
        "user": "Frank",
        "movie": "Inception",
        "rating": 5,
        "comment": "Inception is a true masterpiece of modern cinema. The storytelling is innovative and the cinematography is stunning.",
        "status": "active"
    },
    {
        "_id": 7,
        "user": "Grace",
        "movie": "Inception",
        "rating": 4,
        "comment": "Inception boasts an intricate plot and stunning visual effects. It's a cinematic journey like no other.",
        "status": "active"
    },
    {
        "_id": 8,
        "user": "Heidi",
        "movie": "The Godfather",
        "rating": 5,
        "comment": "The Godfather is an all-time classic. The storytelling is compelling and the characters are unforgettable.",
        "status": "active"
    },
    {
        "_id": 9,
        "user": "Ivan",
        "movie": "Inception",
        "rating": 4,
        "comment": "Inception is a thrilling ride. It keeps you on the edge of your seat from start to finish.",
        "status": "active"
    },
    {
        "_id": 10,
        "user": "Judy",
        "movie": "Inception",
        "rating": 3,
        "comment": "Inception offers exceptional storytelling and visuals, but the plot can be a bit hard to follow at times.",
        "status": "active"
    },
]
mongo_db.client["mydatabase"]["mycollection"].insert_many(DATASET)

# Add the custom operator
mongo_db.add_custom_operator('$prompt', prompt_operator)

# Define the aggregation pipeline using the custom operator
pipeline = [
    {
        '$match': {
            'rating': {'$gt': 3},
            'status': 'active'
        }
    },
    {
        '$project': {
            'user': 1,
            'movie': 1,
            'rating': 1,
            'comment': 1,
            'summary': {
                '$prompt': ['comment', 'Summarize the following movie comment in 5 words:']
            },
            'sentiment': {
                '$prompt': ['comment', 'Respond with the sentiment for the following comment in exactly 1 word:`"positive"` or `"neutral"` or `"negative"`:']
            }
        }
    },
    {
        '$sort': {'rating': -1}
    }
]

# Apply the custom operator
try:
    output = mongo_db.aggregate(pipeline)
except Exception as e:
    logging.error(f"Aggregation failed: {e}")
    output = []

# Print the results
for doc in output:
    print(f"\nDocument ID {doc.get('_id')}:")
    print(f"User: {doc.get('user')}")
    print(f"Movie: {doc.get('movie')}")
    print(f"Rating: {doc.get('rating')}")
    print(f"Comment: {doc.get('comment')}")
    print(f"Summary: {doc.get('summary')}")
    print(f"Sentiment: {doc.get('sentiment')}")


"""
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST http://127.0.0.1:11434/api/chat "HTTP/1.1 200 OK"

Document ID 672391900dd624aa7e7e797b:
User: Alice
Movie: Inception
Rating: 5
Comment: This movie is absolutely amazing! The plot twists and turns in ways you would never expect. Truly a masterpiece.
Summary: Movie reviewer praises its masterpieces.
Sentiment: positive

Document ID 672391900dd624aa7e7e797e:
User: Frank
Movie: Inception
Rating: 5
Comment: Inception is a true masterpiece of modern cinema. The storytelling is innovative and the cinematography is stunning.
Summary: "Inception is a cinematic masterpiece."
Sentiment: positive

Document ID 672391900dd624aa7e7e7980:
User: Heidi
Movie: The Godfather
Rating: 5
Comment: The Godfather is an all-time classic. The storytelling is compelling and the characters are unforgettable.
Summary: Classic movie with great storytelling.
Sentiment: positive

Document ID 672391900dd624aa7e7e797c:
User: Bob
Movie: The Matrix
Rating: 4
Comment: The Matrix offers great visuals. The special effects are truly groundbreaking, making it a visual feast.
Summary: Visuals and effects in Matrix.
Sentiment: positive

Document ID 672391900dd624aa7e7e797d:
User: Diana
Movie: Inception
Rating: 4
Comment: Inception is a good movie, but it can be quite confusing. The plot is complex and requires your full attention.
Summary: Inception is confusing but worth watching.
Sentiment: positive

Document ID 672391900dd624aa7e7e797f:
User: Grace
Movie: Inception
Rating: 4
Comment: Inception boasts an intricate plot and stunning visual effects. It's a cinematic journey like no other.
Summary: "Inception has great visual effects."
Sentiment: positive

Document ID 672391900dd624aa7e7e7981:
User: Ivan
Movie: Inception
Rating: 4
Comment: Inception is a thrilling ride. It keeps you on the edge of your seat from start to finish.
Summary: Highly engaging action movie experience.
Sentiment: positive
"""
