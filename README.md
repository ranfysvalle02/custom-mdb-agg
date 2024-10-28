# custom-mdb-agg

# **Extending MongoDB's Aggregation Framework with Custom Operators and Language Models**

*Unlocking new possibilities in data processing and analysis*

---

**Table of Contents**

1. [Introduction](#introduction)
2. [The Power of Extending the Aggregation Framework](#the-power-of-extending-the-aggregation-framework)
3. [Understanding the Magic Behind Custom Operators](#understanding-the-magic-behind-custom-operators)
4. [Implementing Custom Operators in MongoDB](#implementing-custom-operators-in-mongodb)
    - [Setting Up the Dataset](#setting-up-the-dataset)
    - [Creating the CustomMongoDB Class](#creating-the-custommongodb-class)
    - [Defining the `$prompt` Operator](#defining-the-prompt-operator)
    - [Constructing the Aggregation Pipeline](#constructing-the-aggregation-pipeline)
5. [The Funky Stuff: How It All Works](#the-funky-stuff-how-it-all-works)
6. [Leveraging MongoDB Magic for Quick Experimentation](#leveraging-mongodb-magic-for-quick-experimentation)
7. [Comparing with SuperDuperDB and MindsDB](#comparing-with-superduperdb-and-mindsdb)
8. [Real-World Use Cases](#real-world-use-cases)
9. [Conclusion](#conclusion)

---

## **Introduction**

In the realm of data processing, MongoDB's aggregation framework stands out as a powerful tool for transforming and analyzing data directly within the database. But what if we could extend this framework even further? What if we could integrate advanced language models into our aggregation pipelines, allowing for dynamic text summarization, sentiment analysis, and more?

In this blog post, we'll explore how to **extend MongoDB's aggregation framework** with custom operators that harness the power of language models. We'll dive into the implementation details, uncover the magic behind the scenes, and compare this approach with tools like **SuperDuperDB** and **MindsDB**.

---

## **The Power of Extending the Aggregation Framework**

The aggregation framework in MongoDB allows for sophisticated data manipulation and aggregation operations. By extending it with custom operators, we unlock a new level of flexibility:

- **Custom Processing**: Perform operations tailored to specific application needs.
- **Integration with Machine Learning**: Seamlessly incorporate language models and other ML tools.
- **Enhanced Analytics**: Extract deeper insights from data, such as sentiments, summaries, and entities.
- **Streamlined Workflows**: Keep data processing within the database, reducing the need for external tools.

---

## **Understanding the Magic Behind Custom Operators**

At the heart of this extension lies the ability to define **custom aggregation operators**. These operators can be placed anywhere in the pipeline, allowing for flexible and powerful data transformations.

**Key Concepts:**

- **Sub-Collections**: Temporary collections created during the pipeline execution to handle intermediate results.
- **Custom Operator Functions**: User-defined functions that specify how custom operators process data.
- **Pipeline Execution Flow**: The mechanism that identifies and processes stages containing custom operators.

By leveraging these concepts, we can inject complex logic, such as language model interactions, directly into our aggregation pipelines.

---

## **Implementing Custom Operators in MongoDB**

Let's walk through a practical example to see how this all comes together.

### **Setting Up the Dataset**

We'll use a sample dataset of movie reviews:

```python
from pymongo import MongoClient

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

# Initialize MongoDB client and insert the dataset
client = MongoClient("mongodb://localhost:27017/")
db = client['mydatabase']
collection = db['mycollection']
collection.delete_many({})
collection.insert_many(DATASET)
```

### **Creating the CustomMongoDB Class**

We create a `CustomMongoDB` class to handle custom operators within the aggregation pipeline:

```python
import uuid

class CustomMongoDB:
    def __init__(self, collection):
        self.collection = collection
        self.custom_operators = {}
        self.temp_prefix = 'temp'

    def add_custom_operator(self, name, func):
        self.custom_operators[name] = func

    def aggregate(self, pipeline):
        current_collection = self.collection
        temp_collections = []
        sub_pipeline = []

        for stage in pipeline:
            if not self.contains_custom_operator(stage):
                sub_pipeline.append(stage)
            else:
                if sub_pipeline:
                    current_collection = self.execute_sub_pipeline(current_collection, sub_pipeline)
                    sub_pipeline = []
                    temp_collections.append(current_collection)
                documents = list(current_collection.find())
                documents = self.process_custom_stage(documents, stage)
                temp_collection_name = f"{self.temp_prefix}_{uuid.uuid4().hex}"
                temp_collection = self.collection.database[temp_collection_name]
                temp_collection.insert_many(documents)
                current_collection = temp_collection
                temp_collections.append(current_collection)

        if sub_pipeline:
            current_collection = self.execute_sub_pipeline(current_collection, sub_pipeline)
            temp_collections.append(current_collection)

        results = list(current_collection.find())

        # Clean up temporary collections
        for temp_col in temp_collections:
            if temp_col != self.collection:
                temp_col.drop()

        return results

    # Additional methods for operator handling...
```

**Key Methods:**

- `add_custom_operator`: Registers a custom operator.
- `aggregate`: Processes the pipeline, handling both standard and custom operators.
- `contains_custom_operator`: Checks if a pipeline stage contains any custom operators.
- `execute_sub_pipeline`: Executes standard pipeline stages on the current collection.
- `process_custom_stage`: Processes stages with custom operators.

### **Defining the `$prompt` Operator**

We define a custom `$prompt` operator to interact with a language model:

```python
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

**Assumptions:**

- `language_model.generate(prompt)` is a placeholder for the actual language model interaction.
- You can use models like OpenAI's GPT-3, Hugging Face models, or any other suitable language model.

### **Constructing the Aggregation Pipeline**

We build an aggregation pipeline that utilizes our custom operator:

```python
pipeline = [
    {
        '$match': {
            'rating': {'$gte': 4},
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
                '$prompt': ['comment', 'Provide a brief summary of this review:']
            },
            'sentiment': {
                '$prompt': ['comment', 'What is the sentiment of this review (positive, neutral, negative)?']
            }
        }
    },
    {
        '$sort': {'rating': -1}
    }
]
```

**Explanation:**

- **`$match` Stage**: Filters documents with active status and ratings of 4 or higher.
- **`$project` Stage**: Projects desired fields and uses `$prompt` to generate summaries and sentiments.
- **`$sort` Stage**: Sorts the results by rating in descending order.

---

## **The Funky Stuff: How It All Works**

Let's uncover the magic happening behind the scenes:

1. **Pipeline Processing**:
   - The `aggregate` method processes the pipeline stages.
   - Standard stages are executed using MongoDB's aggregation framework.
   - When a custom operator is encountered, the pipeline execution pauses.

2. **Handling Custom Operators**:
   - The current documents are fetched from the collection.
   - The custom operator function (`prompt_operator`) processes each document.
   - Results are stored in a temporary sub-collection.

3. **Sub-Collections**:
   - Sub-collections are used to store intermediate results after custom processing.
   - This approach allows the pipeline to continue processing as if it were operating on a standard collection.
   - Temporary collections are cleaned up after the aggregation completes.

4. **Seamless Integration**:
   - The combination of standard and custom stages works seamlessly.
   - Users can position custom operators anywhere in the pipeline for maximum flexibility.

---

## **Leveraging MongoDB Magic for Quick Experimentation**

By extending the aggregation framework, we leverage MongoDB's strengths:

- **Powerful Querying**: Utilize rich query capabilities to filter and manipulate data.
- **Aggregation Operators**: Combine standard operators with custom ones for complex transformations.
- **Schema Flexibility**: MongoDB's document model adapts to new fields added by custom operators.
- **Performance**: Keep data processing close to the data, reducing latency and overhead.

**Rapid Experimentation**:

- **Adjust On-the-Fly**: Modify custom operators and pipeline stages without altering the underlying data model.
- **Test New Ideas**: Quickly prototype and test new data processing techniques.
- **Scalable Solutions**: Apply these concepts to large datasets with minimal changes.

---

## **Comparing with SuperDuperDB and MindsDB**

**SuperDuperDB** and **MindsDB** are tools that integrate machine learning models directly into databases. Here's how our approach compares:

### **Similarities**

- **Integrated ML Capabilities**: All approaches bring machine learning into the database layer.
- **Enhanced Functionality**: Enable advanced data processing tasks like predictions and natural language understanding.

### **Differences**

- **Flexibility with Custom Operators**:
  - Our method allows for custom operators that can be defined and modified as needed.
  - SuperDuperDB and MindsDB may have predefined integrations that might be less flexible.

- **Control Over Execution Flow**:
  - By handling the aggregation pipeline ourselves, we have granular control over how and when custom processing occurs.
  - This can be advantageous for complex or specialized use cases.

- **Simplicity and Lightweight Integration**:
  - Our approach doesn't require additional layers or dependencies beyond what's necessary for the language model.
  - SuperDuperDB and MindsDB may introduce additional complexity.

**When to Use Each Approach**:

- **Custom Operators in MongoDB**:
  - Ideal for developers who want full control and flexibility.
  - Suitable for rapid experimentation and bespoke solutions.

- **SuperDuperDB/MindsDB**:
  - Better for out-of-the-box solutions with built-in integrations.
  - Useful when standard ML tasks suffice, and ease of use is a priority.

---

## **Real-World Use Cases**

### **1. E-commerce Product Reviews**

- **Challenge**: Summarize customer reviews and analyze sentiments to improve products.
- **Solution**: Use custom operators to generate summaries and sentiment scores directly within MongoDB.
- **Benefit**: Real-time insights without the need for external processing pipelines.

### **2. Social Media Monitoring**

- **Challenge**: Monitor and analyze user-generated content for trends and public opinion.
- **Solution**: Integrate language models to classify topics and sentiments in posts.
- **Benefit**: Stay ahead of trends and manage brand reputation effectively.

### **3. Customer Support Automation**

- **Challenge**: Triage support tickets and prioritize critical issues.
- **Solution**: Automatically analyze ticket content to determine urgency and assign categories.
- **Benefit**: Improve response times and customer satisfaction.

### **4. Content Personalization**

- **Challenge**: Deliver personalized content recommendations to users.
- **Solution**: Analyze user interactions and preferences using language models.
- **Benefit**: Enhance user engagement and retention.

---

## **Conclusion**

Extending MongoDB's aggregation framework with custom operators and language models opens up a world of possibilities. By integrating advanced processing directly into your database queries, you can:

- **Enhance Data Insights**: Extract meaningful information from unstructured data.
- **Streamline Workflows**: Reduce complexity by keeping data processing within the database.
- **Accelerate Innovation**: Quickly prototype and deploy new data processing techniques.
- **Maintain Flexibility**: Customize operators and pipelines to suit your specific needs.

As we stand at the intersection of data management and artificial intelligence, the boundaries of what's possible are expanding. This fusion challenges us to rethink our approach to data—not just as static information but as a living, breathing entity capable of providing deep insights.

**What if your database could not only store data but also understand and interpret it?**

By pushing the limits of MongoDB's aggregation framework, we're venturing into a realm where data processing becomes more intuitive, intelligent, and integrated. This approach invites us to explore uncharted territories, to question traditional methods, and to innovate beyond conventional boundaries.

The journey doesn't end here. It's an open-ended adventure filled with opportunities to experiment, to create, and to redefine the way we interact with data. The tools are in our hands, and the potential is vast.

**So, will you take the leap?**

---

*Embrace the challenge. Push the boundaries. Let's shape the future together.*

---
