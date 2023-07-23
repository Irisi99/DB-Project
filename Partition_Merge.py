import sys
import gc
import time

dictionary_file = '100k_dictionary.txt'
tuples_file = '100k_int.txt'
result_file = 'result_partition_100k'

# Options are 100k (default) or 10M
if len(sys.argv) == 2:
    if (sys.argv[1] == '10M'):
        dictionary_file = '10M_dictionary_new.txt'
        tuples_file = '10M_int.txt'
        result_file = 'result_partition_10M'


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

del tuples
del tables
gc.collect()


def partition_and_merge(table1, index1, table2, index2, final):
    # initialize the sub-tables
    grouped_table1 = [[]] * len(dictionary)
    grouped_table2 = [[]] * len(dictionary)
    result = []

    for i in table1:
        # if that table for that join index has no rows then initialize it to the row
        if grouped_table1[i[index1]] == []:
            grouped_table1[i[index1]] = [i]
        # if it has rows append the current row
        else:
            grouped_table1[i[index1]].append(i)

    for i in table2:
        # if that table for that join index has no rows then initialize it to the row
        if grouped_table2[i[index2]] == []:
            grouped_table2[i[index2]] = [i]
        # if it has rows append the current row
        else:
            grouped_table2[i[index2]].append(i)

    if final:
        global length
        # open result file as 'write'
        with open(result_file, 'w') as f:
            # iterate over all the strings in the dictionary
            for key in range(len(dictionary)):
                # if there are no rows on any of the tables then continue
                if len(grouped_table1[key]) == 0 or len(grouped_table2[key]) == 0:
                    continue
                # if there are rows then match each row form sub-table1 with each of the rows from the sub-table2
                for r in grouped_table1[key]:
                    for s in grouped_table2[key]:
                        length += 1
                        # write the row to the result file
                        f.write(
                            f"{dictionary[r[0]]} {dictionary[r[1]]} {dictionary[r[2]]} {dictionary[r[3]]} {dictionary[s[1-index2]]} \n")
            f.close()

    else:
        # iterate over all the strings in the dictionary
        for key in range(len(dictionary)):
            # if there are no rows on any of the tables then continue
            if len(grouped_table1[key]) == 0 or len(grouped_table2[key]) == 0:
                continue
            # if there are rows then match each row form sub-table1 with each of the rows from the sub-table2
            for r in grouped_table1[key]:
                for s in grouped_table2[key]:
                    row = r.copy()
                    row.append(s[1-index2])
                    # add row to the result table
                    result.append(row)

        return result


def partition_merge_join():
    # join follows table and frinedOf table
    temp = partition_and_merge(follows_table, 1, friendOf_table, 0, False)
    # join the previous result with likes table
    temp = partition_and_merge(temp, 2, likes_table, 0, False)
    # join the previous result with hasReview table
    partition_and_merge(temp, 3, hasReview_table, 0, True)


# Start timer
start_time = time.perf_counter()
# Execute sort merge join
partition_merge_join()
print(length)
# Stop timer
end_time = time.perf_counter()
print(f"Sort Merge join finished in {end_time - start_time:0.2f} seconds")
