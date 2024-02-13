#-------------------------------------------------------------------------
# AUTHOR: Priyanshu Shekhar
# FILENAME: indexing.py
# SPECIFICATION: Tf-IDF Matrix 
# FOR: CS 4250- Assignment #1
# TIME SPENT: 2.5 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays

#Importing some Python libraries
import csv
import math
documents = []



#Reading the data in a csv file
with open('collection.csv', 'r') as csvfile:
  reader = csv.reader(csvfile)
  for i, row in enumerate(reader):
         if i > 0:  # skipping the header
            documents.append (row[0])


#Conducting stopword removal. Hint: use a set to define your stopwords.
#--> add your Python code here
stopWords = {"i", "she", "and", "the", "they", "her", "their"}

#Conducting stemming. Hint: use a dictionary to map word variations to their stem.
#--> add your Python code here
stemming = {
  "cats" : "cat",
  "dogs" : "dog",
  "love" : "love",
  "loves" : "love",
  
}

# Stemming and stopword removal for each document
stemmed_documents = []
for document in documents:
    stemmed_document = []
    words = document.lower().split()
    for word in words:
        if word not in stopWords:
            if word in stemming:
                stemmed_word = stemming[word]
            else:
                stemmed_word = word
            stemmed_document.append(stemmed_word)
    stemmed_documents.append(" ".join(stemmed_document))

#Identifying the index terms.
#--> add your Python code here
terms = []
for document in stemmed_documents:
    words = document.split()
    for word in words:
        if word not in terms:
            terms.append(word)

#Building the document-term matrix by using the tf-idf weights.
#--> add your Python code here
docTermMatrix = []
for document in stemmed_documents:
    tf = []
    words = document.split()
    for term in terms:
        count = words.count(term)
        if count == 0:
            tf.append(0)
        else:
            tf.append(count/len(words))
    
    docTermMatrix.append(tf)

#Printing the document-term matrix.
#--> add your Python code here
    
#print(docTermMatrix)
#Computing inverse document frequency (IDF) for each term
N = len(documents)
idf_vector = []
for term in terms:
    df = 0
    for document in stemmed_documents:
        words = document.split()
        if term in words:
            df+=1
    idf = math.log((N / df), 10)
    idf_vector.append(idf)


#print(idf_vector)
# Computing tf-idf matrix
tfidf_matrix = []
for tf_vector in docTermMatrix:
    tfidf_vector = [tf * idf for tf, idf in zip(tf_vector, idf_vector)]
    tfidf_matrix.append(tfidf_vector)

#print (tfidf_matrix)
# Printing the tf-idf matrix
print("\nTF-IDF Matrix:")
# Print column headers
print("Terms\t", end="\t")
for term in terms:
    print(term, end="\t")
print()

# Print TF-IDF matrix
for i, document in enumerate(stemmed_documents):
    print(f"Document {i + 1}:", end="\t")
    for term, tfidf in zip(terms, tfidf_matrix[i]):
        print(f"{tfidf:.2f}", end="\t")
    print()