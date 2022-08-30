def q1(mystring):
    """ split the string by tabs to get an array and return the array """
    return mystring.split("\t")

def q2(mystring):
    """ split the string by tabs to get an array and return the second element of the array """
    return mystring.split("\t")[1]

def q3(myarray):
    """ myarray is an list of pairs. this function should return the sum of the first
    items in the pair and the sum of the second items """
    x = sum([tuple[0] for tuple in myarray])
    y = sum([tuple[1] for tuple in myarray])
    return (x,y)

def q4(mystringarray):
    """ return the position of the first occurrence of the string 'hi' or -1 if it is not found.
    you cannot change how the array is iterated and you cannot use any list operations on mystringarray"""

    counter = 0
    for mystring in mystringarray:
        if mystring == "hi":
            break
        else:
            counter += 1 
            continue
    return counter

def q5(myarray):
    """ return a dictionary containing the counts of items in the input array """
    letter_dict = {}
    for letter in myarray:
        if letter not in letter_dict:
            letter_dict[letter] = 1
        else:
            letter_dict[letter] += 1
    return letter_dict

print(q5(['c','a','d','a','e','i']))