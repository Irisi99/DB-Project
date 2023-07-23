import sys
import gc
import time
from collections import defaultdict

dictionary_file = '100k_dictionary.txt'
tuples_file = '100k_int.txt'
result_file = 'result_hash_100k'

# Options are 100k (default) or 10M
if len(sys.argv) == 2:
    if (sys.argv[1] == '10M'):
        dictionary_file = '10M_dictionary_new.txt'
        tuples_file = '10M_int.txt'
        result_file = 'result_hash_10M'

# index 0 : subject
# index 1 : type/relationship
# index 2 : object
tuples = []
# Index of string is its respective int
dictionary = [0]
length = 0

# open text file in 'read' mode
with open(dictionary_file, 'r') as f:
    # read line from file
    line = f.readline()
    # continue until end of file
    while line != '':
        # remove trailing chars such as . or \n
        line = line[:-1]
        # split string an value
        line = line.split(':')
        # add only string sice value is it's index in the list
        dictionary.append(line[0])
        line = f.readline()
    # close file
    f.close()

with open(tuples_file, 'r') as f:
    line = f.readline()
    while line != '':
        line = line[:-1] 
        line = line.split(' ')
        # add tuple as list with 3 elements
        tuples.append(line)
        line = f.readline()
    f.close()

# Assume we have N different types
N = len(tuples)
# index x is table with type x based on dictionary
tables = [[]] * N

for tuple in tuples:
    # find index of this tuple's type table
    index = int(tuple[1])
    if tables[index] == []:
        # if table does not exist yet create it with one element
        tables[index] = [[int(tuple[0]), int(tuple[2])]]
    else:
        # if table exist add the new row with subject and object
        tables[index].append([int(tuple[0]), int(tuple[2])])

# find indexes of the tables we will be using
follows_table = tables[dictionary.index('follows')]
friendOf_table = tables[dictionary.index('friendOf')]
likes_table = tables[dictionary.index('likes')]
hasReview_table = tables[dictionary.index('hasReview')]

def generator(table):
    for i in table:
        yield i

follows_gen = generator(follows_table)
friendOf_gen = generator(friendOf_table)
likes_gen = generator(likes_table)
hasReview_gen = generator(hasReview_table)

del follows_table
del likes_table
del friendOf_table
del hasReview_table
del tuples
del tables
gc.collect()


def hash_join():

    # hash function for integers returns the integer so we skip it
    
    h = defaultdict(list)
    for s in friendOf_gen:
        # add frinedOf.Subject as key index into the hash table and append all frinedOf.Object as its value
        h[s[0]].append(s[1])

    # match key of hash table with follows.Object
    # add friendOf.Object to the result table
    temp_gen = generator([[r[0], r[1], s]
                         for r in follows_gen for s in h[r[1]]])

    h = defaultdict(list)
    for s in likes_gen:
        # add likes.Subject as key index into the hash table and append all likes.Object as its value
        h[s[0]].append(s[1])

    # match key of hash table with friendOf.Object
    # add likes.Object to the result table
    temp_gen = generator([[r[0], r[1], r[2], s]
                         for r in temp_gen for s in h[r[2]]])

    h = defaultdict(list)
    for s in hasReview_gen:
        # add hasReview.Subject as key index into the hash table and append all hasReview.Object as its value
        h[s[0]].append(s[1])

    global length
    # open result file as 'write'
    with open(result_file, 'w') as f:
        for r in temp_gen:
            # match key of hash table with likes.Object
            for s in h[r[3]]:
                length += 1
                # add hasReview.Object to the result
                # save result into file
                f.write(
                    f"{dictionary[r[0]]} {dictionary[r[1]]} {dictionary[r[2]]} {dictionary[r[3]]} {dictionary[s]} \n")
        f.close()


# Start timer
start_time_total = time.perf_counter()
# Execute hash merge join
hash_join()
print(length)
# Stop timer
end_time_total = time.perf_counter()
print(
    f"Hash join finished in {end_time_total - start_time_total:0.2f} seconds")
