#-------------------------------------------------------------------------
# AUTHOR: Priyanshu Shekhar
# FILENAME: index_mongo.py
# SPECIFICATION: mongodb CRUD instructions
# FOR: CS 4250- Assignment #3
# TIME SPENT: 3-4hrs.
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays
from pymongo import MongoClient
import re


def connectDataBase():
    # Create a database connection object using pymongo
    client = MongoClient('mongodb://localhost:27017/')
    db = client['corpus']  # Replace 'your_database_name' with your actual database name
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary indexed by term to count how many times each term appears in the document.
    term_count = {}
    for term in docText.lower().split(" "):
        term_count[term] = term_count.get(term, 0) + 1

    # create a list of objects to include full term objects.
    terms_list = []
    for term, count in term_count.items():
        terms_list.append({"term": term, "num_chars": len(term), "term_count": count})

    # produce a final document as a dictionary including all the required document fields
    document = {
        "_id": int(docId),
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": {"id": 1, "name": docCat},
        "index": terms_list
    }

    # insert the document
    col.insert_one(document)

def deleteDocument(col, docId):
    # Delete the document from the database
    col.delete_one({"_id": int(docId)})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count.
    index = {}
    documents = col.find({}, {"index": 1, "title": 1})
    for doc in documents:
        title = doc.get("title", "Unknown")
        for term_obj in doc["index"]:
            term = term_obj["term"].lower()
            term = re.sub(r'[^\w\s]', '', term)  # Remove punctuation marks
            count = term_obj["term_count"]
            if term in index:
                index[term].append(f"{title}:{count}")
            else:
                index[term] = [f"{title}:{count}"]

    # Combine counts for terms appearing in multiple documents
    for term, counts in index.items():
        if len(set(counts)) > 1:
            index[term] = ','.join(counts)

    return index
