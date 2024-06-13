from datetime import datetime
from elasticsearch import Elasticsearch

# Connect to Elasticsearch running locally (assuming default configuration)
es = Elasticsearch(['http://localhost:9200'])


def create_index(index_name):
    """
    Creates an Elasticsearch index with mappings.

    Args:
        index_name (str): Name of the Elasticsearch index.
    """
    try:
        index_settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "taxi_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "location": {
                        "properties": {
                            "longitude": {"type": "float"},
                            "latitude": {"type": "float"}
                        }
                    }
                }
            }
        }

        # Create the index with defined settings and mappings
        es.indices.create(index=index_name, body=index_settings)
        print(f"Index '{index_name}' created successfully.")
    except Exception as e:
        print(f"Error creating index '{index_name}': {e}")


def index_document(index_name, doc_id, document):
    """
    Indexes a document in Elasticsearch.

    Args:
        index_name (str): Name of the Elasticsearch index.
        doc_id (str or int): Document ID.
        document (dict): Document data to index.
    """
    try:
        res = es.index(index=index_name, id=doc_id, body=document)
        print(f"Document indexed successfully: {res}")
    except Exception as e:
        print(f"Error indexing document: {e}")


def search_documents(index_name, query):
    """
    Searches documents in Elasticsearch.

    Args:
        index_name (str): Name of the Elasticsearch index to search.
        query (dict): Elasticsearch query DSL.

    Returns:
        dict: Elasticsearch response.
    """
    try:
        res = es.search(index=index_name, body=query)
        return res
    except Exception as e:
        print(f"Error searching documents: {e}")


if __name__ == "__main__":
    # Example usage: Create an index with mappings
    create_index("flink-index")

    # Example document to index
    document = {
        "taxi_id": "2289",
        "timestamp": datetime.now(),
        "location": {
            "longitude": 116.58038,
            "latitude": 40.07628
        }
    }

    # Example usage: Index document into Elasticsearch
    index_document("flink-index", 1, document)

    # Example search query
    query = {
        "query": {
            "match": {
                "taxi_id": "2289"
            }
        }
    }

    # Example usage: Search documents in Elasticsearch
    search_result = search_documents("flink-index", query)
    print("Search result:")
    print(search_result)
