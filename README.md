![](mdb.png)

# Extending MongoDB's Aggregation Framework with Custom Operators and Language Models

*Unlock new dimensions in data processing and analysis.*

---

*Inspired by [prompt-dolla](https://github.com/ranfysvalle02/prompt-dolla)*

---

**Table of Contents**

1. [Introduction](#introduction)
2. [Motivation for Custom Operators](#motivation-for-custom-operators)
3. [Understanding the Implementation](#understanding-the-implementation)
   - [Setting Up the Dataset](#setting-up-the-dataset)
   - [Initializing the `CustomMongoAggregator` Class](#initializing-the-custommongoaggregator-class)
   - [Defining the `$prompt` Operator](#defining-the-prompt-operator)
   - [Constructing the Aggregation Pipeline](#constructing-the-aggregation-pipeline)
   - [Executing the Pipeline](#executing-the-pipeline)
4. [How It Works](#how-it-works)
5. [Benefits of This Approach](#benefits-of-this-approach)
6. [Comparing with MindsDB and SuperDuperDB](#comparing-with-mindsdb-and-superduperdb)
7. [Conclusion](#conclusion)
8. [Appendix: Security Considerations](#appendix-security-considerations)

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

We start by setting up a sample dataset of movie reviews and inserting it into our MongoDB collection:

```python
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

# Clear existing data
mongo_db.client["mydatabase"]["mycollection"].delete_many({})

# Define the dataset
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

# Insert the dataset
mongo_db.client["mydatabase"]["mycollection"].insert_many(DATASET)
```

This code:

- Imports necessary modules and classes.
- Configures logging.
- Initializes the `CustomMongoAggregator`.
- Clears any existing data in the collection.
- Inserts the dataset into MongoDB.

### Initializing the `CustomMongoAggregator` Class

The `CustomMongoAggregator` class is part of the `custom_mdb_agg` package, which extends MongoDB's aggregation framework to support custom operators. It handles:

- **Custom Operator Registration**: Allows you to add or remove custom operators.
- **Aggregation Processing**: Executes the aggregation pipeline, handling both standard and custom operators.
- **Temporary Collections**: Manages temporary collections created during the aggregation process.

**Note**: The full implementation of `CustomMongoAggregator` can be found in the `custom_mdb_agg` package.

### Defining the `$prompt` Operator

We define a custom `$prompt` operator that interfaces with a language model:

```python
from custom_mdb_agg.operators import prompt_operator

# Add the custom operator
mongo_db.add_custom_operator('$prompt', prompt_operator)
```

The `prompt_operator` function is responsible for:

- Extracting the specified field from each document.
- Constructing a prompt using the provided prompt text.
- Interacting with the language model to generate a response.
- Returning the response to be included in the aggregation result.

### Constructing the Aggregation Pipeline

We build an aggregation pipeline that utilizes the custom `$prompt` operator:

```python
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
                '$prompt': [
                    'comment',
                    'Summarize the following movie comment in 5 words:'
                ]
            },
            'sentiment': {
                '$prompt': [
                    'comment',
                    'Respond with the sentiment for the following comment in exactly 1 word: "positive", "neutral", or "negative":'
                ]
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

### Executing the Pipeline

We execute the pipeline and handle any potential exceptions:

```python
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
```

This code:

- Executes the aggregation pipeline.
- Catches and logs any exceptions that occur.
- Iterates over the results and prints out each document's details, including the generated summary and sentiment.

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

## Appendix: Security Considerations

When integrating custom operators and external services like LLMs into your data processing pipeline, it's crucial to address various security aspects to protect your data and system integrity. Below are key security considerations related to the provided code:

### 1. **Data Privacy and Protection**

#### **Sensitive Data Exposure**

- **Issue**: The code sends document data, including potentially sensitive fields, to an external service (the LLM via the `ollama` client).
- **Consideration**:
  - **Data Minimization**: Only include necessary data in the prompts sent to the LLM. Avoid sending unnecessary fields or entire documents if not required.
  - **Anonymization**: Remove or obfuscate personally identifiable information (PII) before sending data to the LLM.
  - **Compliance**: Ensure compliance with data protection regulations like GDPR, HIPAA, or other local laws.

#### **Transmission Security**

- **Issue**: Data transmitted to the LLM service may be intercepted if not properly secured.
- **Consideration**:
  - **Encryption**: Use secure communication protocols (e.g., HTTPS, SSL/TLS) when communicating with the LLM service to encrypt data in transit.
  - **Secure Channels**: Verify that the `ollama` client uses secure channels for data transmission.

### 2. **Authentication and Authorization**

#### **LLM Service Authentication**

- **Issue**: Unauthorized access to the LLM service could lead to misuse or data leakage.
- **Consideration**:
  - **API Keys and Credentials**: Store API keys or tokens securely, using environment variables or secure credential storage systems.
  - **Access Control**: Ensure that only authorized applications or users can access the LLM service.

#### **Database Access**

- **Issue**: Unauthorized access to the MongoDB database can compromise data integrity and confidentiality.
- **Consideration**:
  - **Authentication**: Use MongoDB's authentication mechanisms to restrict database access.
  - **Role-Based Access Control (RBAC)**: Assign appropriate roles and permissions to users and applications accessing the database.
  - **Connection Strings**: Securely manage MongoDB connection strings, avoiding hardcoding credentials in the code.

### 3. **Injection Attacks**

#### **Prompt Injection**

- **Issue**: Malicious data in the `comment` field could manipulate the LLM's output or behavior.
- **Consideration**:
  - **Input Validation**: Sanitize and validate input data before including it in prompts.
  - **Prompt Design**: Design prompts to minimize the impact of injected content, possibly by setting strict formatting or using placeholders.

### 4. **Denial of Service (DoS)**

#### **Resource Exhaustion**

- **Issue**: Processing a large number of documents with LLM calls can strain system resources or lead to service rate limiting.
- **Consideration**:
  - **Rate Limiting**: Implement rate limiting to control the number of requests to the LLM service.
  - **Concurrency Control**: Manage concurrent LLM calls to prevent overloading the system.
  - **Error Handling**: Handle service unavailability or throttling responses gracefully.

### 5. **Logging and Monitoring**

#### **Sensitive Data in Logs**

- **Issue**: Logging sensitive information can lead to data breaches if logs are accessed by unauthorized parties.
- **Consideration**:
  - **Log Sanitization**: Exclude or mask sensitive data in logs.
  - **Secure Log Storage**: Ensure logs are stored securely with proper access controls.

### 6. **Configuration Management**

#### **Environment Variables and Secrets**

- **Issue**: Hardcoding sensitive configurations can lead to exposure.
- **Consideration**:
  - **Secure Storage**: Use environment variables or secret management tools to store sensitive configurations.
  - **Version Control**: Exclude sensitive information from version control systems.

### 7. **Compliance and Legal**

#### **Data Residency**

- **Issue**: Sending data to external services may violate data residency requirements.
- **Consideration**:
  - **Service Location**: Ensure that the LLM service complies with data residency laws applicable to your data.
  - **Agreements**: Review service agreements for compliance with legal obligations.

### 8. **Secure Coding Practices**

#### **Error Handling**

- **Issue**: Unhandled exceptions can lead to application crashes or expose stack traces.
- **Consideration**:
  - **Graceful Degradation**: Implement robust error handling to manage exceptions without exposing sensitive information.
  - **User Feedback**: Provide generic error messages without revealing internal details.

#### **Input Validation**

- **Issue**: Invalid input can cause unexpected behavior or security vulnerabilities.
- **Consideration**:
  - **Type Checking**: Ensure inputs are of expected types and formats.
  - **Boundary Checking**: Validate input lengths and ranges to prevent buffer overflows or similar issues.

### 9. **Security Testing**

#### **Regular Assessments**

- **Issue**: Undetected vulnerabilities can persist without regular testing.
- **Consideration**:
  - **Penetration Testing**: Conduct regular security assessments to identify and remediate vulnerabilities.
  - **Code Reviews**: Perform peer reviews focusing on security aspects.

### 10. **Encryption and Data Storage**

#### **Data at Rest**

- **Issue**: Storing sensitive data unencrypted can lead to data breaches.
- **Consideration**:
  - **Encryption**: Use encryption for sensitive data stored in databases or files.
  - **Access Controls**: Restrict access to sensitive data based on roles and responsibilities.

Incorporating security best practices is essential when integrating custom operators and external services into your data processing pipelines. By addressing the considerations outlined above, you can enhance the security posture of your application, protect user data, and ensure compliance with relevant regulations.

---

## OUTPUT

```
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
```
