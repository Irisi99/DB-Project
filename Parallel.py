import math
import multiprocessing
import itertools
# from operator import itemgetter
# import time


def merge(*args):
    # Support explicit left/right args, as well as a two-item
    # tuple which works more cleanly with multiprocessing.
    if len(args) == 1:
        key = args[0][-1]
        left, right = args[0][0]
    else:
        left = args[0]
        right = args[0] if isinstance(args[1], int) else args[1]
        key = args[1] if len(args) == 2 else args[2]

    left_length, right_length = len(left), len(right)
    left_index, right_index = 0, 0
    merged = []
    while left_index < left_length and right_index < right_length:
        if left[left_index][key] <= right[right_index][key]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
    if left_index == left_length:
        merged.extend(right[right_index:])
    else:
        merged.extend(left[left_index:])
    return merged


def merge_sort(data, key):
    length = len(data)
    if length <= 1:
        return data
    middle = length / 2
    left = merge_sort(data[:int(middle)], key)
    right = merge_sort(data[int(middle):], key)
    return merge(left, right, key)


def sort_parallel(data, key):
    # Creates a pool of worker processes, one per CPU core.
    # We then split the initial data into partitions, sized
    # equally per worker, and perform a regular merge sort
    # across each partition.
    processes = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=processes)
    size = int(math.ceil(float(len(data)) / processes))
    data = [data[i * size:(i + 1) * size] for i in range(processes)]
    # data = pool.map(partial(merge_sort, key=key), data)
    data = pool.starmap(merge_sort, zip(data, itertools.repeat(key)))
    # Each partition is now sorted - we now just merge pairs of these
    # together using the worker pool, until the partitions are reduced
    # down to a single sorted result.
    while len(data) > 1:
        # If the number of partitions remaining is odd, we pop off the
        # last one and append it back after one iteration of this loop,
        # since we're only interested in pairs of partitions to merge.
        extra = data.pop() if len(data) % 2 == 1 else None
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, zip(data, itertools.repeat(key))
                        ) + ([extra] if extra else [])
    return data[0]


# dictionary_file = '100k_dictionary.txt'
# tuples_file = '100k_int.txt'

# tuples = []
# # Index of string is its respective int
# dictionary = [0]
# # Result of the SQL query
# result = []

# # open text file in 'read' mode
# with open(dictionary_file, 'r') as f:
#     # read line from file
#     line = f.readline()
#     # continue until end of file
#     while line != '':
#         # remove trailing chars such as . or \n
#         line = line[:-1]
#         # split string an value
#         line = line.split(':')
#         # add only string sice value is it's index in the list
#         dictionary.append(line[0])
#         line = f.readline()
#     # close file
#     f.close()

# with open(tuples_file, 'r') as f:
#     line = f.readline()
#     while line != '':
#         line = line[:-1]
#         line = line.split(' ')
#         # add tuple as list with 3 elements
#         tuples.append(line)
#         line = f.readline()
#     f.close()

# if __name__ == "__main__":
#     # size = 1000000
#     # data_unsorted = [random.randint(0, size) for _ in range(size)]
#     start = time.time()
#     data_sorted = sort_parallel(tuples, 0)
#     end = time.time() - start
#     print(f"parallel sort time: {end:0.4f}")

#     start = time.time()
#     data_sorted = sorted(tuples, key=itemgetter(0))
#     end = time.time() - start
#     print(f"normal sort time: {end:0.4f}")
