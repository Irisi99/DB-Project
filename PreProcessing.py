# index 0 : subject
# index 1 : type/relationship
# index 2 : object
tuples = []
# Index of string is its respective int
dictionary = []

# open 100k dataset in 'read' mode
with open('100k.txt', 'r') as f:
    line = f.readline()
    while line != '':
        # remove trailing chars such as . or \n
        line = line[:-3]
        line = line.split('\t')
        temp = []
        # remove any other character other than the value like wsdbm:, sorg:, gn: etc.
        for i in line:
            temp.append(i[i.find(':')+1:])
        tuples.append(temp)
        line = f.readline()
    f.close()

# create the dictionary for the 100k dataset
with open('100k_dictionary.txt', 'w') as f:
    index = 1
    # iterate over all the tuples and its elements
    for i in tuples:
        for j in i:
            # check if string is already in the dictionary, if it is continue
            if j in dictionary:
                continue
            # if it is not present in the dictionary then add it to the array as well as writing it to the dictionary file
            else:
                dictionary.append(j)
                f.write(j+':'+str(index)+'\n')
                index += 1
    f.close()

# create the new 100k dataset file that will hold the integer representatives
with open('100k_int.txt', 'w') as f:
    # iterate over the touples and write them to the file but instead of the element write their corresponding index on the dictionary
    for i in tuples:
        f.write(str(dictionary.index(
            i[0])+1)+' '+str(dictionary.index(i[1])+1)+' '+str(dictionary.index(i[2])+1)+'\n')
    f.close()

dictionary = []

# open the 10M dictionary file
with open('10M_dictionary.txt', 'r') as f:
    line = f.readline()
    while line != '':
        # remove trailing chars such as . or \n
        line = line[:-1]
        line = line.split(':')
        temp = []
        # since the dictionary is currently in the form of an url for some of the elements we parse it and include only the value
        for i in line:
            if i.find('http') != -1:
                continue
            if i.find('/') != -1:
                i = i[i.rfind('/')+1:-1]
            if i.find('#') != -1:
                i = i[i.find('#')+1:]
            temp.append(i)
        # add the parsed value to the new dictionary array
        dictionary.append(temp)
        line = f.readline()
    f.close()

# create the new dictionary file that will have the parsed values
with open('10M_dictionary_new.txt', 'w') as f:
    for i in dictionary:
        f.write(i[0]+':'+i[1]+'\n')
    f.close()
