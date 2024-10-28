from pymongo import MongoClient
import ollama
import uuid

desiredModel = 'llama3.2:3b'

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

class CustomMongoDB:
    def __init__(self, uri, database, collection, temp_prefix='temp'):
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
        self.collection.delete_many({})
        self.collection.insert_many(DATASET)
        self.custom_operators = {}
        self.temp_prefix = temp_prefix

    def add_custom_operator(self, name, func):
        """Add a custom operator to the collection."""
        self.custom_operators[name] = func

    def remove_custom_operator(self, name):
        """Remove a custom operator from the collection."""
        if name in self.custom_operators:
            del self.custom_operators[name]

    def contains_custom_operator(self, stage):
        """Check if a pipeline stage contains any custom operators."""
        def check_expr(expr):
            if isinstance(expr, dict):
                for key, value in expr.items():
                    if key in self.custom_operators:
                        return True
                    elif isinstance(value, (dict, list)):
                        if check_expr(value):
                            return True
            elif isinstance(expr, list):
                for item in expr:
                    if check_expr(item):
                        return True
            return False
        return check_expr(stage)

    def aggregate(self, pipeline):
        """Execute an aggregation pipeline with custom operators processed where needed."""
        current_collection = self.collection
        temp_collections = []

        pipeline_iter = iter(pipeline)
        sub_pipeline = []
        for stage in pipeline_iter:
            if not self.contains_custom_operator(stage):
                sub_pipeline.append(stage)
            else:
                # Execute the accumulated sub_pipeline in MongoDB
                if sub_pipeline:
                    current_collection = self.execute_sub_pipeline(current_collection, sub_pipeline)
                    sub_pipeline = []
                    temp_collections.append(current_collection)

                # Process the custom stage
                # Fetch the documents
                documents = list(current_collection.find())

                # Process the custom stage per document
                documents = self.process_custom_stage(documents, stage)

                # Write the documents to a new temporary collection
                temp_collection_name = f"{self.temp_prefix}_{uuid.uuid4().hex}"
                temp_collection = self.db[temp_collection_name]
                temp_collection.insert_many(documents)
                current_collection = temp_collection
                temp_collections.append(current_collection)

        # After processing all stages, if sub_pipeline is not empty, execute it
        if sub_pipeline:
            current_collection = self.execute_sub_pipeline(current_collection, sub_pipeline)
            temp_collections.append(current_collection)

        # Fetch the final results
        results = list(current_collection.find())

        # Clean up temporary collections
        for temp_col in temp_collections:
            if temp_col != self.collection:
                temp_col.drop()

        return results

    def execute_sub_pipeline(self, collection, pipeline):
        """Execute a sub-pipeline on the given collection."""
        temp_collection_name = f"temp_{uuid.uuid4().hex}"
        temp_collection = self.db[temp_collection_name]
        pipeline_with_out = pipeline + [{'$out': temp_collection_name}]
        collection.aggregate(pipeline_with_out)
        return temp_collection

    def process_custom_stage(self, documents, stage):
        """Process a custom stage per document."""
        # We assume the stage is a dict with a single key
        operator, expr = next(iter(stage.items()))
        if operator == '$project':
            processed_docs = []
            for doc in documents:
                new_doc = {'_id': doc['_id']}
                for key, value in expr.items():
                    if isinstance(value, int) and value == 1:
                        new_doc[key] = doc.get(key)
                    else:
                        new_doc[key] = self.process_expr(value, doc)
                processed_docs.append(new_doc)
            return processed_docs
        else:
            raise NotImplementedError(f"Custom processing for operator {operator} is not implemented.")

    def process_expr(self, expr, doc):
        """Recursively process an expression within a document context."""
        if isinstance(expr, dict):
            if len(expr) == 1:
                key, value = next(iter(expr.items()))
                if key in self.custom_operators:
                    return self.custom_operators[key](doc, value)
                elif key.startswith('$'):
                    return self.evaluate_operator(key, value, doc)
                else:
                    return {key: self.process_expr(value, doc)}
            else:
                return {k: self.process_expr(v, doc) for k, v in expr.items()}
        elif isinstance(expr, list):
            return [self.process_expr(item, doc) for item in expr]
        elif isinstance(expr, str) and expr.startswith('$'):
            return self.get_field_value(doc, expr[1:])
        else:
            return expr

    def evaluate_operator(self, operator, value, doc):
        """Evaluate standard MongoDB operators."""
        if operator == '$concat':
            parts = self.process_expr(value, doc)
            return ''.join(str(part) for part in parts)
        elif operator == '$strLenCP':
            string = self.process_expr(value, doc)
            return len(string)
        else:
            raise NotImplementedError(f"Operator {operator} not implemented.")

    def get_field_value(self, doc, field_path):
        """Retrieve the value of a field from the document given a field path."""
        fields = field_path.split('.')
        value = doc
        for f in fields:
            if isinstance(value, dict) and f in value:
                value = value[f]
            else:
                return None
        return value

# Custom operator function
def prompt_operator(doc, args):
    field = args[0]
    prompt_text = args[1]
    # Get the value from the document
    field_name = field  # Field name without '$'
    field_value = doc.get(field_name)
    if field_value is None:
        return None
    # Call the LLM with the field value and prompt text
    print(f"""
    [prompt]
    {prompt_text}
    [/prompt]
    [context]
    field: {field_name}
    value:
    {str(field_value)}
    [full document]
    {str(doc)}
    [/full document]
    [/context]
    """,)
    response = ollama.chat(model=desiredModel, messages=[
        {
            'role': 'user',
            'content': f"""
[prompt]
{prompt_text}
[/prompt]
[context]
field: {field_name}
value:
{str(field_value)}
[full document]
{str(doc)}
[/full document]
[/context]
""",
        },
    ])
    return response['message']['content']

if __name__ == "__main__":
    # Initialize the CustomMongoDB class
    mongo_db = CustomMongoDB("mongodb://localhost:27017/?directConnection=true", "mydatabase", "mycollection")

    # Add the custom operator
    mongo_db.add_custom_operator('$prompt', prompt_operator)

    # Define the aggregation pipeline using the custom operator anywhere
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
    output = mongo_db.aggregate(pipeline)

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
Document ID 1:
User: Alice
Movie: Inception
Rating: 5
Comment: This movie is absolutely amazing! The plot twists and turns in ways you would never expect. Truly a masterpiece.
Summary: "Inception is an amazing masterpiece."
Sentiment: positive

Document ID 6:
User: Frank
Movie: Inception
Rating: 5
Comment: Inception is a true masterpiece of modern cinema. The storytelling is innovative and the cinematography is stunning.
Summary: Praises Inception as a masterpiece.
Sentiment: positive

Document ID 8:
User: Heidi
Movie: The Godfather
Rating: 5
Comment: The Godfather is an all-time classic. The storytelling is compelling and the characters are unforgettable.
Summary: Classic film with memorable characters.
Sentiment: positive

Document ID 2:
User: Bob
Movie: The Matrix
Rating: 4
Comment: The Matrix offers great visuals. The special effects are truly groundbreaking, making it a visual feast.
Summary: Visually stunning sci-fi movie experience.
Sentiment: positive

Document ID 4:
User: Diana
Movie: Inception
Rating: 4
Comment: Inception is a good movie, but it can be quite confusing. The plot is complex and requires your full attention.
Summary: Confusing but good mental puzzle film.
Sentiment: neutral

Document ID 7:
User: Grace
Movie: Inception
Rating: 4
Comment: Inception boasts an intricate plot and stunning visual effects. It's a cinematic journey like no other.
Summary: "Intricate plot with impressive visuals."
Sentiment: positive

Document ID 9:
User: Ivan
Movie: Inception
Rating: 4
Comment: Inception is a thrilling ride. It keeps you on the edge of your seat from start to finish.
Summary: Thrilling and suspenseful action film.
Sentiment: positive
"""
