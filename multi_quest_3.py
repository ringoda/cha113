from __future__ import division
import sqlite3
import string
import time
import multiprocessing
import heapq

#Function to measure and return average depth of comments in a subreddit
def measure_subreddit(subreddit_id):
	#Execute the query to get all id and parent IDs for comments in a subreddit
	c.execute(""" SELECT id, parent_id  
				  FROM comments
				  WHERE subreddit_id=:id""", {"id": subreddit_id})
	
	#Placeholder for our pairs where a comment's ID is the key and a comment's parent ID is the value 
	pair_dict = {}
	
	#Loop through the results from the query
	for query_res in c.fetchall():
		#Collect all IDs in our dict
		pair_dict[query_res[0]] = query_res[1]

	#Placeholder for our results where a comment's ID is the key and the deepest 'thread' is the value 
	result_dict = {}
	
	#Algorithm that goes through our pairs and finds the depth of each top-level comment:
	#This is done by jumping from ID to parent ID to ID until a top-level comment is reached
	# we use a counter to count how far we've gone and once we have reached the end we check
	# whether this number is the deepest (highest) we have so far for this top-level comment
	# if it is we overwrite the previous value with this one otherwise we just ignore it and
	# move onto the next ID. Top level comments have 't3' as the beginning of their parent ID.
	for i in pair_dict:
		counter = 0
		next_key = pair_dict[i]
		if next_key[:2] == 't3':
			if i in result_dict:
				if counter > result_dict[i]:
					result_dict[i] = counter
					continue
				continue
			else:
				result_dict[i] = counter
				continue
		while True:
			if next_key in pair_dict:
				counter += 1
				if pair_dict[next_key][:2] == 't3':
					if next_key in result_dict:
						if counter > result_dict[next_key]:
							result_dict[next_key] = counter
							break
						break
					else:
						result_dict[next_key] = counter
						break
				else:
					next_key = pair_dict[next_key]
			else:
				break

	#If to make sure we don't divide by zero if there are no comments in the subreddit
	if not result_dict:
		return (0,subreddit_id)
	#Return the average and the subreddit ID
	return ((sum(result_dict.values())/len(result_dict)), subreddit_id)
		



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

	#Map the threads to our function above with the ids as parameter, set chunksize to 4717
	result += p.map(measure_subreddit, sub_ids.keys(), chunksize=4717)

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