import uuid
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError

class CustomMongoAggregator:
    """
    A class to extend MongoDB aggregation framework with custom operators.

    This class allows users to define custom operators and use them within aggregation pipelines.
    It processes the pipeline, separating stages that contain custom operators, and handles them accordingly.

    Parameters:
    - uri (str): MongoDB connection URI.
    - database (str): Database name.
    - collection (str): Collection name.
    - temp_prefix (str): Prefix for temporary collections (default 'temp').

    """

    def __init__(self, uri, database, collection, temp_prefix='temp'):
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
        self.custom_operators = {}
        self.temp_prefix = temp_prefix
        self.logger = logging.getLogger(__name__)

    def add_custom_operator(self, name, func):
        """
        Add a custom operator to the collection.

        Parameters:
        - name (str): The name of the custom operator.
        - func (callable): The function implementing the custom operator.
        """
        if not name.startswith('$'):
            raise ValueError("Custom operator name must start with '$'")
        self.custom_operators[name] = func

    def remove_custom_operator(self, name):
        """
        Remove a custom operator from the collection.

        Parameters:
        - name (str): The name of the custom operator to remove.
        """
        if name in self.custom_operators:
            del self.custom_operators[name]

    def contains_custom_operator(self, stage):
        """
        Check if a pipeline stage contains any custom operators.

        Parameters:
        - stage (dict): The aggregation pipeline stage to check.

        Returns:
        - bool: True if the stage contains custom operators, False otherwise.
        """
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
        """
        Execute an aggregation pipeline with custom operators processed where needed.

        Parameters:
        - pipeline (list): The aggregation pipeline.

        Returns:
        - list: The result of the aggregation.
        """
        current_collection = self.collection
        temp_collections = []

        try:
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
                    if documents:
                        temp_collection.insert_many(documents)
                    current_collection = temp_collection
                    temp_collections.append(current_collection)

            # After processing all stages, if sub_pipeline is not empty, execute it
            if sub_pipeline:
                current_collection = self.execute_sub_pipeline(current_collection, sub_pipeline)
                temp_collections.append(current_collection)

            # Fetch the final results
            results = list(current_collection.find())

        except PyMongoError as e:
            self.logger.error(f"MongoDB Error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected Error: {e}")
            raise
        finally:
            # Clean up temporary collections
            for temp_col in temp_collections:
                if temp_col != self.collection:
                    try:
                        temp_col.drop()
                    except Exception as e:
                        self.logger.warning(f"Failed to drop temporary collection {temp_col.name}: {e}")

        return results

    def execute_sub_pipeline(self, collection, pipeline):
        """
        Execute a sub-pipeline on the given collection.

        Parameters:
        - collection (Collection): The MongoDB collection to execute the pipeline on.
        - pipeline (list): The aggregation pipeline.

        Returns:
        - Collection: The temporary collection with the results.
        """
        temp_collection_name = f"{self.temp_prefix}_{uuid.uuid4().hex}"
        temp_collection = self.db[temp_collection_name]
        pipeline_with_out = pipeline + [{'$out': temp_collection_name}]
        try:
            collection.aggregate(pipeline_with_out)
        except PyMongoError as e:
            self.logger.error(f"Error executing pipeline: {e}")
            raise
        return temp_collection

    def process_custom_stage(self, documents, stage):
        """
        Process a custom stage per document.

        Parameters:
        - documents (list): List of documents to process.
        - stage (dict): The aggregation pipeline stage.

        Returns:
        - list: The list of processed documents.
        """
        operator, expr = next(iter(stage.items()))
        if operator == '$project' or operator == '$addFields':
            processed_docs = []
            for doc in documents:
                new_doc = {}
                for key, value in expr.items():
                    if isinstance(value, int) and value == 1:
                        new_doc[key] = doc.get(key)
                    elif value == 0:
                        continue  # Exclude the field
                    else:
                        new_doc[key] = self.process_expr(value, doc)
                processed_docs.append(new_doc)
            return processed_docs
        else:
            raise NotImplementedError(f"Custom processing for operator {operator} is not implemented.")

    def process_expr(self, expr, doc):
        """
        Recursively process an expression within a document context.

        Parameters:
        - expr: The expression to process.
        - doc (dict): The document context.

        Returns:
        - The result of processing the expression.
        """
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
        """
        Evaluate standard MongoDB operators.

        Parameters:
        - operator (str): The operator name.
        - value: The operator value.
        - doc (dict): The document context.

        Returns:
        - The result of the operator.
        """
        if operator == '$concat':
            parts = self.process_expr(value, doc)
            return ''.join(str(part) for part in parts)
        elif operator == '$strLenCP':
            string = self.process_expr(value, doc)
            return len(string)
        elif operator == '$toUpper':
            string = self.process_expr(value, doc)
            return str(string).upper()
        elif operator == '$toLower':
            string = self.process_expr(value, doc)
            return str(string).lower()
        elif operator == '$substr':
            args = self.process_expr(value, doc)
            if len(args) != 3:
                raise ValueError("$substr requires three arguments")
            string, start, length = args
            return string[start:start+length]
        else:
            raise NotImplementedError(f"Operator {operator} not implemented.")

    def get_field_value(self, doc, field_path):
        """
        Retrieve the value of a field from the document given a field path.

        Parameters:
        - doc (dict): The document.
        - field_path (str): The field path.

        Returns:
        - The field value.
        """
        fields = field_path.split('.')
        value = doc
        for f in fields:
            if isinstance(value, dict) and f in value:
                value = value[f]
            else:
                return None
        return value
