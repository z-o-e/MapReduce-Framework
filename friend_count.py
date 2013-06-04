import MapReduce
import sys

"""
    A MapReduce algorithm to count the number of friends each person has within a 
    simple social network dataset consisting of key-value pairs where each key is 
    a person and each value is a friend of that person. 
    
    Can text result with data in 'friends.json'
"""

mr = MapReduce.MapReduce()


def mapper(friends):
    # key: person id
    key = friends[0]
    mr.emit_intermediate(key, 1)

def reducer(key, friends):
    # key: person id
    # friends: number of friend counts
    total = 0
    for v in friends:
      total += v
    mr.emit((key, total))



if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)