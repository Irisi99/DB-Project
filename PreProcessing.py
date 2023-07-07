
touples = []
dictionary = []

with open('100k.txt', 'r') as f:
    line = f.readline()
    while line != '' :
        line = line[:-3]
        line = line.split('\t')
        temp = []
        for i in line:
            temp.append(i[i.find(':')+1:])
        touples.append(temp)
        line = f.readline()
    f.close()
    
with open('100k_dictionary.txt', 'w') as f:
    index = 1
    for i in touples:
        for j in i:
            if j in dictionary:
                continue
            else:
                dictionary.append(j)
                f.write(j+':'+str(index)+'\n')
                index += 1
    f.close()

with open('100k_int.txt', 'w') as f:
    for i in touples:
        f.write(str(dictionary.index(i[0])+1)+' '+str(dictionary.index(i[1])+1)+' '+str(dictionary.index(i[2])+1)+'\n')
    f.close()

dictionary = []
    
with open('10M_dictionary.txt', 'r') as f:
        line = f.readline()
        while line != '' :
            line = line[:-1]
            line = line.split(':')
            temp = []
            for i in line:
                if i.find('http') != -1:
                    continue
                if i.find('/') != -1:
                    i = i[i.rfind('/')+1:-1]
                if i.find('#') != -1:
                    i = i[i.find('#')+1:]
                temp.append(i)
            dictionary.append(temp)
            line = f.readline()
        f.close()
        
with open('10M_dictionary_new.txt', 'w') as f:
        for i in dictionary:
            f.write(i[0]+':'+i[1]+'\n')
        f.close()