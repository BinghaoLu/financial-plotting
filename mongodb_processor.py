import uuid
from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDBProcessor:
    def __init__(self, connection_string, db_name, source_collection, target_collection):
        self.client = MongoClient(connection_string)
        self.db = self.client[db_name]
        self.source_collection = self.db[source_collection]
        self.target_collection = self.db[target_collection]
    
    @staticmethod
    def normalize_analyst_output(data, column_name=None, keys_to_select=['article_db_id','artical_db_id','published_datetime','pair','category_name'], key_rename_map=None):
        """
        Processes and normalizes nested lists of dictionaries in JSON-like data,
        while selecting specific keys, renaming keys, adding a unique signal ID,
        and excluding the `_id` field.
        
        Args:
            data (list): Input list of dictionaries.
            column_name (str, optional): The column containing nested lists of dictionaries.
            keys_to_select (list, optional): List of keys to keep in the final output.
            key_rename_map (dict, optional): Dictionary mapping old key names to new key names.
        
        Returns:
            list: A list of dictionaries with normalized, filtered, and updated records.
        """
        if not isinstance(data, list):
            raise ValueError("Input data must be a list of dictionaries.")
        
        # Determine the column to process
        if column_name:
            if not any(column_name in record for record in data):
                raise ValueError(f"Column '{column_name}' not found in the data.")
        else:
            # Default to known columns
            if any('analyst_consensus_output' in record for record in data):
                column_name = 'analyst_consensus_output'
            elif any('analyst_outputs' in record for record in data):
                column_name = 'analyst_outputs'
            else:
                raise ValueError("Neither 'analyst_consensus_output' nor 'analyst_outputs' found in the data.")
        
        normalized_data = []
        for record in data:
            if column_name in record and isinstance(record[column_name], list):
                for nested in record[column_name]:
                    flattened_record = {**record}  # Copy the original record
                    flattened_record.pop(column_name, None)  # Remove the original nested column
                    flattened_record.pop("_id", None)  # Remove `_id` field if present
                    if isinstance(nested, dict):
                        flattened_record.update(nested)
                    
                    # Select specific keys if provided
                    if keys_to_select:
                        flattened_record = {k: v for k, v in flattened_record.items() if k in keys_to_select}
                    
                    # Rename keys if a renaming map is provided
                    if key_rename_map:
                        flattened_record = {key_rename_map.get(k, k): v for k, v in flattened_record.items()}
                    
                    # Add a unique signal ID
                    flattened_record["signal_id"] = str(uuid.uuid4())  # Generate a UUID as signal ID
                    
                    normalized_data.append(flattened_record)
        
        return normalized_data

    def process_existing_data(self):
        """
        Process all existing data in the source collection without normalization.
        """
        print("Processing existing data...")
        cursor = self.source_collection.find()
        data = list(cursor)
        data = self.normalize_analyst_output(data)
   
        try:
            self.target_collection.insert_many(data)
            print(f"Processed and inserted {len(data)} existing documents.")
        except PyMongoError as e:
                print(f"Error occurred during insert_many: {e}")

    def start_change_stream(self):
        """
        Start listening to changes in the source collection and process new data.
        """
        try:
            print("Listening for new changes...")
            with self.source_collection.watch() as stream:
                for change in stream:
                    if change["operationType"] == "insert":
                        new_document = change["fullDocument"]
                        try:
                            self.target_collection.insert_one(new_document)
                            print(f"Processed and inserted a document")
                        except ValueError as e:
                            print(f"Insertion error: {e}")
        except PyMongoError as e:
            print(f"Error occurred during change stream: {e}")
        finally:
            self.client.close()
            print("Connection closed.")

    def run(self):
        """
        Process existing data and then start listening for changes.
        """
        self.process_existing_data()
        self.start_change_stream()

if __name__ == '__main__':
    processor = MongoDBProcessor(
    connection_string="",
    db_name="galpha",
    source_collection="signals_v2",
    target_collection="signals_v2_stats"
    )

    # Start the processor
    processor.run()