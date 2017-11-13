import sqlite3
import string
import time
import multiprocessing
import heapq
import itertools

#Function to count common authors given a pair of subreddit ids
def measure_subreddit(subreddit_id):
	#Get the number of common authors given a pair of IDs
	return (len(author_ids[subreddit_id[0]].intersection(author_ids[subreddit_id[1]])), subreddit_id)
	
#Extract values from sql query
def get_info(val):
	#Get the values
	return (val[0],val[1])

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

	#Dictionary which has a subreddit ID as key and a list of author IDs as value
	author_ids = {}

	#Placeholder for the pairs we need to compare
	subreddit_ids = []
	
	#Temp list to hold results from our multiprocessing
	tmp_list = []

	#Start time measuring for this query
	start1 = time.time()

	#Get the million first comments
	c.execute(""" SELECT subreddit_id, author_id
			 	  FROM comments LIMIT 1000000""")
	
	#Create a pool of 10 threads
	p = multiprocessing.Pool(10)

	#Map our threads and the query result to the get_info function
	tmp_list += p.map(get_info, c.fetchall(), chunksize=60000)
	
	#Create the dictionary that holds all data
	for i in tmp_list:
		if i[0] in author_ids:
			author_ids[i[0]].add(i[1])
		else:
			author_ids[i[0]] = set(i[1])
	
	#Use itertools to create all possible pairs from our dictionary of author IDs
	subreddit_ids = list(itertools.combinations(author_ids.keys(), 2))
	
	print "First query: " + str(time.time() - start1)
	print "Number of comparisons: " + str(len(subreddit_ids))
	
	#Create a multiprocessing pool of 10 threads
	p = multiprocessing.Pool(10)

	#Map the threads to our function above with the pairs of ids as parameter, set chunksize to 1000000
	result += p.map(measure_subreddit, subreddit_ids, chunksize=1000000)

	#Find the top10
	top10 = heapq.nlargest(10, result)

	#Get our threshold value as the minimum value in the top10 list
	threshold = top10[-1][0]

	print "Min max value: " + str(threshold)
	
	#Placeholder for our results
	result = []

	#Dictionary which has a subreddit ID as key and a list of author IDs as value
	author_ids = {}

	#Placeholder for the pairs we need to compare
	subreddit_ids = []

	#Temp list to hold results from our multiprocessing
	tmp_list = []

	#Start time measuring for this query
	start1 = time.time()

	#Query for all author_ids and subreddit_ids
	c.execute(""" SELECT subreddit_id, author_id
			 	  FROM comments""")

	#Create a multiprocessing pool of 10 threads
	p = multiprocessing.Pool(10)

	#Map our threads and the query result to the get_info function	
	tmp_list += p.map(get_info, c.fetchall(), chunksize=100000)
	
	#Create the dictionary that holds all data
	for i in tmp_list:
		if i[0] in author_ids:
			author_ids[i[0]].add(i[1])
		else:
			author_ids[i[0]] = set(i[1])

	print "Dict size before: " + str(len(author_ids))

	#Lets remove from our dictionary all subreddit_ids that have less distinct comments than our threshold
	for k, v in author_ids.items():
		if len(v) < threshold:
			del author_ids[k]

	print "Dict size after: " + str(len(author_ids))
	
	#Create a list containing all possible pairs from our dictionary
	subreddit_ids = list(itertools.combinations(author_ids.keys(), 2))
	print "Second query: " + str(time.time() - start1)
	print "Number of pairs to check: " + str(len(subreddit_ids))
	
	#Create a multiprocessing pool of 10 threads
	p = multiprocessing.Pool(10)

	#Map the threads to our function above with the pair of ids as parameter, set chunksize to 1000000
	result += p.map(measure_subreddit, subreddit_ids, chunksize=1000000)

	#Find the top10
	top10 = heapq.nlargest(10, result)
	
	#Loop through the result
	for i in top10:
		pair = []
		c.execute(""" SELECT name, id
			 	  	  FROM subreddits
			 	  	  WHERE id=:id1 OR id=:id2""", {"id1": i[1][0], "id2": i[1][1]})
		for res in c.fetchall():
			pair.append((res[0],res[1]))
		#Print the values
		print str(i[0]) + '\t' + pair[1][0] + '\t' + pair[1][1] + '\t' + pair[0][0] + '\t' + pair[0][1]
	

	#Stop measuring
	end = time.time()

	#Print the runtime
	print 'Runtime: ' + str(end-start)