![](mdb.png)

# Extending MongoDB's Aggregation Framework with Custom Operators and Language Models

*Unlock new dimensions in data processing and analysis.*

---

**Table of Contents**

1. [Introduction](#introduction)
2. [Motivation for Custom Operators](#motivation-for-custom-operators)
3. [Understanding the Implementation](#understanding-the-implementation)
   - [Setting Up the Dataset](#setting-up-the-dataset)
   - [Creating the `CustomMongoDB` Class](#creating-the-custommongodb-class)
   - [Defining the `$prompt` Operator](#defining-the-prompt-operator)
   - [Constructing the Aggregation Pipeline](#constructing-the-aggregation-pipeline)
4. [How It Works](#how-it-works)
5. [Benefits of This Approach](#benefits-of-this-approach)
6. [Comparing with MindsDB and SuperDuperDB](#comparing-with-mindsdb-and-superduperdb)
7. [Conclusion](#conclusion)

---

![](https://i.sstatic.net/S555v.png)

## Introduction

MongoDB's aggregation framework is a powerful tool for data transformation and analysis. However, when dealing with advanced text processing or machine learning tasks, the built-in operators might not suffice. What if we could extend this framework to integrate advanced language models directly into our aggregation pipelines?

In this blog post, we'll explore how to **extend MongoDB's aggregation framework** by incorporating custom operators that leverage language models. This approach enables dynamic text summarization, sentiment analysis, and more—all within your MongoDB queries.

---

## Motivation for Custom Operators

While MongoDB provides a rich set of aggregation operators, there are scenarios where we need functionality beyond what's available:

- **Advanced Text Processing**: Tasks like summarization, sentiment analysis, and entity recognition.
- **Machine Learning Integration**: Incorporating predictive models directly into data processing pipelines.
- **Customized Operations**: Tailoring data transformations to specific business logic.

By introducing custom operators, we can:

- **Enhance Functionality**: Perform complex operations without external processing.
- **Simplify Architecture**: Reduce the need for additional systems or services.
- **Increase Efficiency**: Process data where it resides, minimizing data movement.
- **Maintain Full Control**: Customize every aspect of data processing to suit specific needs.

---

![](https://cdn.ttgtmedia.com/rms/onlineimages/ai_a199952058.jpg)

## Understanding the Implementation

Let's dive into how we can implement custom operators in MongoDB, specifically integrating a language model for text summarization and sentiment analysis.

### Setting Up the Dataset

We start with a sample dataset of movie reviews:

```python
DATASET = [
    {
        "_id": 1,
        "user": "Alice",
        "movie": "Inception",
        "rating": 5,
        "comment": "This movie is absolutely amazing! The plot twists and turns in ways you would never expect. Truly a masterpiece.",
        "status": "active"
    },
    # ... additional documents ...
]
```

This dataset contains user reviews that we want to analyze using a language model.

### Creating the `CustomMongoDB` Class

We create a `CustomMongoDB` class to handle aggregation with custom operators:

```python
from pymongo import MongoClient
import uuid

class CustomMongoDB:
    def __init__(self, uri, database, collection, temp_prefix='temp'):
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
        # Initialize the collection with the dataset
        self.collection.delete_many({})
        self.collection.insert_many(DATASET)
        self.custom_operators = {}
        self.temp_prefix = temp_prefix

    def add_custom_operator(self, name, func):
        """Add a custom operator to the collection."""
        self.custom_operators[name] = func

    # ... other methods ...
```

**Key Methods:**

- `add_custom_operator(name, func)`: Registers a custom operator.
- `aggregate(pipeline)`: Processes the pipeline, handling both standard and custom operators.
- `contains_custom_operator(stage)`: Checks if a pipeline stage contains any custom operators.
- `execute_sub_pipeline(collection, pipeline)`: Executes standard pipeline stages.
- `process_custom_stage(documents, stage)`: Processes stages with custom operators.

### Defining the `$prompt` Operator

We define a custom `$prompt` operator that interfaces with a language model:

```python
import ollama

desiredModel = 'llama3.2:3b'

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
```

This operator takes a field from the document and a prompt text, then generates a response using the language model.

### Constructing the Aggregation Pipeline

We build an aggregation pipeline that utilizes the custom `$prompt` operator:

```python
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
                '$prompt': ['comment', 'Respond with the sentiment for the following comment in exactly 1 word: "positive", "neutral", or "negative":']
            }
        }
    },
    {
        '$sort': {'rating': -1}
    }
]
```

**Explanation:**

- **`$match` Stage**: Filters active documents with a rating greater than 3.
- **`$project` Stage**: Projects desired fields and uses `$prompt` to generate summaries and sentiments.
- **`$sort` Stage**: Sorts the results by rating in descending order.

---

## How It Works

Here's a step-by-step breakdown of the process:

1. **Pipeline Processing**:
   - The `aggregate` method processes each stage of the pipeline.
   - Standard stages are executed normally.
   - When a custom operator is detected, processing switches to handle it specifically.

2. **Custom Operator Detection**:
   - The method `contains_custom_operator` checks each stage for custom operators.

3. **Handling Custom Operators**:
   - Documents are fetched from the current collection.
   - The custom operator function (`prompt_operator`) is applied to each document.
   - Results are stored in a temporary collection to maintain pipeline flow.

4. **Continuation of Pipeline**:
   - The pipeline resumes with the temporary collection as the new data source.
   - Subsequent stages are processed accordingly.

5. **Cleanup**:
   - Temporary collections are deleted after the aggregation to free up resources.

---

## Benefits of This Approach

- **Seamless Integration**: Custom operators can be used alongside standard aggregation operators.
- **Flexibility**: Place custom operators anywhere in the pipeline.
- **Performance**: Reduce data transfer by processing within the database.
- **Maintainability**: Keep data processing logic in one place, simplifying updates and debugging.
- **Complete Control**: Customize every aspect of data processing, from operator definitions to pipeline execution.

---

## Comparing with MindsDB and SuperDuperDB

**MindsDB** and **SuperDuperDB** are powerful tools that integrate machine learning models directly into databases, including MongoDB. They offer out-of-the-box solutions for incorporating AI into your data workflows. However, there are key differences when compared to our custom aggregation implementation.

### Flexibility and Control

- **Custom Implementation**:
  - **Full Control**: You define exactly how and where the language models are integrated.
  - **Custom Operators**: Tailor-made functions that suit your specific use cases.
  - **No Dependencies**: Minimal reliance on external libraries or frameworks beyond what's necessary.
- **MindsDB/SuperDuperDB**:
  - **Predefined Integrations**: Comes with built-in functionalities that may not cover all unique needs.
  - **Abstraction Layers**: Higher-level interfaces that might limit fine-grained control.

### Integration Complexity

- **Custom Implementation**:
  - **Simplified Architecture**: Directly integrate into existing MongoDB setups.
  - **Learning Curve**: Requires understanding of MongoDB's internals and custom operator creation.
- **MindsDB/SuperDuperDB**:
  - **Additional Layers**: Introduce new components into your architecture.
  - **Ease of Use**: Designed to be user-friendly with less need for in-depth coding.

### Performance Considerations

- **Custom Implementation**:
  - **Optimized for Specific Tasks**: Since you control the implementation, you can optimize for performance.
  - **Resource Management**: Direct oversight of how resources are utilized.
- **MindsDB/SuperDuperDB**:
  - **General-Purpose Solutions**: May not be optimized for all specific scenarios.
  - **Overhead**: Additional layers might introduce performance overhead.

### Use Cases

- **When to Use Custom Implementation**:
  - **Specific Requirements**: Your project has unique needs not met by existing tools.
  - **Full Control Needed**: You want complete oversight of the data processing pipeline.
  - **Customization**: You need to implement custom logic or integrate specialized models.

- **When to Use MindsDB/SuperDuperDB**:
  - **Standard Use Cases**: Common machine learning tasks like forecasting, classification, etc.
  - **Quick Setup**: You prefer rapid integration without delving into custom code.
  - **Support and Community**: Benefit from the support and community around these platforms.

---

## Conclusion

Extending MongoDB's aggregation framework with custom operators and language models unlocks a world of possibilities. By integrating advanced processing directly into your database queries, you can:

- **Enhance Data Insights**: Extract meaningful information from unstructured data.
- **Streamline Workflows**: Reduce complexity by keeping data processing within the database.
- **Accelerate Innovation**: Quickly prototype and deploy new data processing techniques.
- **Maintain Flexibility and Control**: Customize operators and pipelines to suit your specific needs.

While tools like MindsDB and SuperDuperDB offer powerful features and ease of use, building a custom implementation provides unparalleled control and flexibility. You can tailor every aspect of the data processing pipeline, ensuring it fits perfectly with your application's requirements.

As we stand at the intersection of data management and artificial intelligence, the boundaries of what's possible are expanding. This fusion challenges us to rethink our approach to data—not just as static information but as a dynamic entity capable of providing deep insights.

**What if your database could not only store data but also understand and interpret it?**

By pushing the limits of MongoDB's aggregation framework, we're venturing into a realm where data processing becomes more intuitive, intelligent, and integrated. This approach invites us to explore uncharted territories, question traditional methods, and innovate beyond conventional boundaries.

The journey doesn't end here. It's an open-ended adventure filled with opportunities to experiment, create, and redefine the way we interact with data. The tools are in our hands, and the potential is vast.

**So, will you take the leap?**

---

*Embrace the challenge. Push the boundaries. Let's shape the future together.*

---

## FULL CODE

```python
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
```
