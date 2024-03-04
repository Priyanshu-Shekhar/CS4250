#-------------------------------------------------------------------------
# AUTHOR: Priyanshu Shekhar
# FILENAME: db_connection.py
# SPECIFICATION: CRUD database
# FOR: CS 4250- Assignment #2
# TIME SPENT: 5 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
import re


def connectDataBase():
    # Create a database connection object using psycopg2
    # --> add your Python code here
    conn = psycopg2.connect(
        dbname="corpus",
        user="postgres",
        password="PShekhar",
        port="5432"
    )
    return conn

# Function to create tables in the database
def createTables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS category (
            id_cat SERIAL PRIMARY KEY,
            name TEXT UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS document (
            id_doc SERIAL PRIMARY KEY,
            text TEXT,
            title TEXT,
            num_chars INTEGER,
            date DATE,
            id_cat INTEGER,
            FOREIGN KEY (id_cat) REFERENCES category(id_cat)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS term (
            term TEXT PRIMARY KEY,
            nums_char INTEGER UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS document_term (
            id_doc INTEGER,
            term TEXT,
            term_count INTEGER,
            FOREIGN KEY (id_doc) REFERENCES document(id_doc),
            FOREIGN KEY (term) REFERENCES term(term)
        )
    """)



def createCategory(cur, catId, catName):
    # Insert a category in the database
    # --> add your Python code here
    createTables(cur) 
    cur.execute("INSERT INTO category (id_cat, name) VALUES (%s, %s)", (catId, catName))


def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    createTables(cur)

    terms = re.findall(r'\b\w+\b', docText.lower())
   
    for term in terms:
        cur.execute("INSERT INTO term (term) VALUES (%s) ON CONFLICT DO NOTHING", (term,))
    
    cur.execute("SELECT id_cat FROM category WHERE name = %s", (docCat,))
   
    catId = cur.fetchone()[0]

    cur.execute("INSERT INTO document (id_doc, text, title, num_chars, date, id_cat) VALUES (%s, %s, %s, %s, %s, %s)",
            (docId, docText, docTitle, len(docText), docDate, catId))

    
    for term in set(terms):
        count = terms.count(term)
        cur.execute("INSERT INTO document_term (term, id_doc, term_count) VALUES ((SELECT term FROM term WHERE term = %s), %s, %s)",
                    (term, docId, count))



def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    cur.execute("DELETE FROM document_term WHERE id_doc = %s", (docId,))

    # 2 Delete the document from the database
    # --> add your Python code here
    cur.execute("DELETE FROM document WHERE id_doc = %s", (docId,))

    


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)
    

def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    cur.execute("SELECT term.term, document.title, document_term.term_count \
                FROM document_term \
                JOIN term ON document_term.term = term.term \
                JOIN document ON document_term.id_doc = document.id_doc \
                ORDER BY term.term" )
    index = {}   
    rows = cur.fetchall()
    for row in rows:
        term = row[0]
        title = row[1]
        count = row[2]
        if term not in index:
            index[term] = f"{title}: {count}"
        else:
            index[term] += f", {title}: {count}"
    return index
