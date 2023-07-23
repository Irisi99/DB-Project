import math
import multiprocessing
import itertools


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
    # while there are still uncehcked elements on the right andleft continue the loop
    while left_index < left_length and right_index < right_length:
        # add the 'smaller' element to the merged list since order is ascending
        if left[left_index][key] <= right[right_index][key]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1

    # if there are unchecked elements on the left/right table add them to the result
    if left_index == left_length:
        merged.extend(right[right_index:])
    else:
        merged.extend(left[left_index:])
    return merged


def merge_sort(data, key):
    length = len(data)
    # if the length of the array/table is 1 then it is already sorted so we return it
    if length <= 1:
        return data
    # if it is not of length one then we sort both halves of it
    middle = length / 2
    left = merge_sort(data[:int(middle)], key)
    right = merge_sort(data[int(middle):], key)
    # after sorting both halves we merge them and return the result
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
