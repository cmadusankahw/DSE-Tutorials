
import re

inputFile = open("D:/Zone24x7/Word-Count-Training/PythonI/inputs/word_count.txt", "r")

wordDictionary = dict()

for line in inputFile:

    line=re.sub("[^A-Za-z]+"," ",line).lower()
    words = line.split()
    for word in words:
        if word in wordDictionary:
            wordDictionary[word] += 1
        else:
            wordDictionary[word] = 1

for key in list(wordDictionary.keys()):
    print(key, "=>", wordDictionary[key])







