import sys
import gc
import time
from collections import defaultdict
from operator import itemgetter
from Parallel import *

dictionary_file = '100k_dictionary.txt'
tuples_file = '100k_int.txt'

# index 0 : object
# index 1 : type/relationship
# index 2 : subject
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

# if tuples_file == '10M_int.txt':
#     tuples = tuples[0:int(len(tuples)/3)]

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
        # if table exist add the new row with object and subject
        tables[index].append([int(tuple[0]), int(tuple[2])])

# find indexes of the tables we will be using
follows_table = tables[dictionary.index('follows')]
friendOf_table = tables[dictionary.index('friendOf')]
likes_table = tables[dictionary.index('likes')]
hasReview_table = tables[dictionary.index('hasReview')]

del tuples
del tables
gc.collect()


def hash_join():

    # hash function for integers returns the integer...
    h = defaultdict(list)
    for s in friendOf_table:
        # add friends.Subject as index into the hash table
        h[s[1]].append(s[0])

    # match with friendOf.Subject
    # add friendOf.Object to the result
    temp = [[r[0], r[1], s] for r in follows_table
            for s in h[r[0]]]

    h = defaultdict(list)
    for s in likes_table:
        # add likes.Object as index into the hash table
        h[s[0]].append(s[1])

    # match with friendOf.Object
    # # add likes.Subject to the result
    temp = [[r[0], r[1], r[2], s] for r in temp for s in h[r[2]]]

    h = defaultdict(list)
    for s in hasReview_table:
        # add hasReview.Object into the hash table
        h[s[0]].append(s[1])

    # match with likes.Subject
    # add hasReview.Object to the result
    return [[r[0], r[1], r[2], r[3], s] for r in temp for s in h[r[3]]]


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


def sort_merge_join():
    # sort follows based on Object
    sorted_follows = sorted(follows_table, key=itemgetter(0))
    # sort friendOf based on Subject
    sorted_friendOf = sorted(friendOf_table, key=itemgetter(1))
    # merge two tables
    temp = merge(sorted_follows, 0, sorted_friendOf, 1)
    del sorted_follows
    del sorted_friendOf
    gc.collect()

    # sort based on firendOf.Object
    sorted_temp = sorted(temp, key=itemgetter(2))
    # sort based on likes.Object
    sorted_likes = sorted(likes_table, key=itemgetter((0)))
    # merge with likes table
    temp = merge(sorted_temp, 2, sorted_likes, 0)
    del sorted_temp
    del sorted_likes
    gc.collect()

    # sort based on likes.Subject
    sorted_temp = sorted(temp, key=itemgetter(3))
    # sort based on hasReview.Object
    sorted_hasReview = sorted(hasReview_table, key=itemgetter(0))
    # merge with hasReview table
    return merge(sorted_temp, 3, sorted_hasReview, 0)


def parallel_sort_merge_join():
    # sort follows based on Object
    sorted_follows = sort_parallel(follows_table, 0)
    # sort friendOf based on Subject
    sorted_friendOf = sort_parallel(friendOf_table, 1)
    temp = merge(sorted_follows, 0, sorted_friendOf, 1)
    # merge two tables
    del sorted_follows
    del sorted_friendOf
    gc.collect

    # sort based on firendOf.Object
    sorted_temp = sort_parallel(temp, 2)
    # sort based on likes.Object
    sorted_likes = sort_parallel(likes_table, 0)
    # merge with likes table
    temp = merge(sorted_temp, 2, sorted_likes, 0)
    del sorted_temp
    del sorted_likes
    gc.collect

    # sort based on likes.Subject
    sorted_temp = sort_parallel(temp, 3)
    # sort based on hasReview.Object
    sorted_hasReview = sort_parallel(hasReview_table, 0)
    # merge with hasReview table
    return merge(sorted_temp, 3, sorted_hasReview, 0)


def partition_and_merge(table1, index1, table2, index2):
    grouped_table1 = [[]] * len(dictionary)
    grouped_table2 = [[]] * len(dictionary)
    result = []

    for i in table1:
        if grouped_table1[i[index1]] == []:
            grouped_table1[i[index1]] = [i]
        else:
            grouped_table1[i[index1]].append(i)
    for i in table2:
        if grouped_table2[i[index2]] == []:
            grouped_table2[i[index2]] = [i]
        else:
            grouped_table2[i[index2]].append(i)

    for key in range(len(dictionary)):
        if len(grouped_table1[key]) == 0 or len(grouped_table2[key]) == 0:
            continue
        for r in grouped_table1[key]:
            for s in grouped_table2[key]:
                row = r.copy()
                row.append(s[1-index2])
                result.append(row)

    return result


def partition_merge_join():
    temp = partition_and_merge(follows_table, 0, friendOf_table, 1)
    temp = partition_and_merge(temp, 2, likes_table, 0)
    return partition_and_merge(temp, 3, hasReview_table, 0)


if __name__ == "__main__":

    # Start timer
    start_time_total = time.perf_counter()
    # Execute hash merge join
    result = hash_join()
    print(len(result))
    # Stop timer
    end_time_total = time.perf_counter()
    print(
        f"Hash join finished in {end_time_total - start_time_total:0.2f} seconds")

    gc.collect()

    # Start timer
    start_time = time.perf_counter()
    # Execute sort merge join
    result = sort_merge_join()
    print(len(result))
    # Stop timer
    end_time = time.perf_counter()
    print(f"Sort Merge join finished in {end_time - start_time:0.2f} seconds")

    gc.collect()

    # Start timer
    start_time = time.perf_counter()
    # Execute sort merge join
    result = parallel_sort_merge_join()
    print(len(result))
    # Stop timer
    end_time = time.perf_counter()
    print(
        f"Parallel Sort Merge join finished in {end_time - start_time:0.2f} seconds")

    gc.collect()

    # Start timer
    start_time = time.perf_counter()
    # Execute sort merge join
    result = partition_merge_join()
    print(len(result))
    # Stop timer
    end_time = time.perf_counter()
    print(
        f"Partition Merge join finished in {end_time - start_time:0.2f} seconds")

# result[:] = [[dictionary[index[0]], dictionary[index[1]], dictionary[index[2]],
#               dictionary[index[3]], dictionary[index[4]]] for index in result]

# print(result[0])
