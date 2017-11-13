import sqlite3
import string
import time
import multiprocessing
import heapq

#Function to count distinct words in a subreddit given the ID of the subreddit
def measure_subreddit(subreddit_id):

	#Execute the query to get all comments in a subreddit
	c.execute(""" SELECT body  
				  FROM comments
				  WHERE subreddit_id=:id""", {"id": subreddit_id})
	
	#Create a string in which we will add all comments
	comment_text = ""
	
	#Loop through the results from the query
	for comment in c.fetchall():
		#Collect all comments in our text
		comment_text += " " + comment[0]

	#Remove punctuations
	translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))
	comment_text = comment_text.translate(translator).lower()

	#Split the text to words
	comment_text = comment_text.split()
	
	#Return the length of the set of comment_text and the id of the subreddit
	return (len(set(comment_text)), subreddit_id)
		



if __name__ == "__main__":
	
	#Start the time measuring
	start = time.time()
	
	#Connect to our database
	conn = sqlite3.connect('reddit.db')

	#Set text_factory to 'str'
	conn.text_factory = str

	#Get a cursor object in the database
	c = conn.cursor()

	#Placeholder for our results
	result = []

	#Dictionary for the results of our first query
	sub_ids = {}

	#Query the database for all ids and names of subreddits
	for subreddit_id in c.execute("""SELECT DISTINCT id,name
									FROM subreddits"""):
		#Add the result to our dictionary where the id is the key and the name is the value
		sub_ids[subreddit_id[0]] = subreddit_id[1]

	#Create a multiprocessing pool of 10 threads
	p = multiprocessing.Pool(10)

	#Map the threads to our function above with the ids as parameter, set chunksize to 30
	result += p.map(measure_subreddit, sub_ids.keys(), chunksize=50)

	#Find the top10
	top10 = heapq.nlargest(10, result)
	
	#Loop through the result
	for i in top10:
		#Print the values
		print(str(i[0]) + '\t' + i[1] + '\t' + sub_ids[i[1]])
	

	#Stop measuring
	end = time.time()

	#Print the runtime
	print('Runtime: ' + str(end-start))