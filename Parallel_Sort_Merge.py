import gc
import time
from Parallel import *

dictionary_file = '100k_dictionary.txt'
tuples_file = '100k_int.txt'

# index 0 : subject
# index 1 : type/relationship
# index 2 : object
tuples = []
# Index of string is its respective int
dictionary = [0]
# Result of the SQL query
result = []

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

del tuples
del tables
gc.collect()


def merge(table1, index1, table2, index2):
    result = []
    outer_i = 0
    inner_i = 0

    while outer_i < len(table1) and inner_i < len(table2):
        temp = inner_i
        # if table2 key is bigger advance in table1
        if table1[outer_i][index1] > table2[temp][index2]:
            inner_i += 1
            continue
        # if table1 key is bigger advance in table2
        if table1[outer_i][index1] < table2[temp][index2]:
            outer_i += 1
            continue
        # if the key matches then
        while table1[outer_i][index1] == table2[temp][index2]:
            # create the new element
            # add the new value from the other table
            row = table1[outer_i].copy()
            row.append(table2[temp][1-index2])
            # add it in the result list
            result.append(row)
            # advance in table 2
            temp += 1
            # if we reached the end then break out of the loop
            if temp == len(table2):
                break
        # advance in table1
        outer_i += 1
        # if next element in table 1 does not match with current element in table2 then advance it table2
        if (temp != len(table2) and outer_i < len(table1)
                and table1[outer_i][index1] != table2[inner_i][index2]):
            inner_i = temp
    return result


def parallel_sort_merge_join():
    # sort follows based on Object
    sorted_follows = sort_parallel(follows_table, 1)
    # sort friendOf based on Subject
    sorted_friendOf = sort_parallel(friendOf_table, 0)
    temp = merge(sorted_follows, 1, sorted_friendOf, 0)
    # merge two tables
    del sorted_follows
    del sorted_friendOf
    gc.collect

    # sort based on firendOf.Object
    sorted_temp = sort_parallel(temp, 2)
    # sort based on likes.Subject
    sorted_likes = sort_parallel(likes_table, 0)
    # merge with likes table
    temp = merge(sorted_temp, 2, sorted_likes, 0)
    del sorted_temp
    del sorted_likes
    gc.collect

    # sort based on likes.Object
    sorted_temp = sort_parallel(temp, 3)
    # sort based on hasReview.Subject
    sorted_hasReview = sort_parallel(hasReview_table, 0)
    # merge with hasReview table
    return merge(sorted_temp, 3, sorted_hasReview, 0)


if __name__ == "__main__":
    # Start timer
    start_time = time.perf_counter()
    # Execute sort merge join
    result = parallel_sort_merge_join()
    print(len(result))
    # Stop timer
    end_time = time.perf_counter()
    print(
        f"Parallel Sort Merge join finished in {end_time - start_time:0.2f} seconds")
