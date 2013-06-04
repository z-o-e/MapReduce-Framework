import MapReduce
import sys

"""
    A MapReduce algorithm to check whether symmetric(mutual-friendship) property 
    holds. Generate a list of all non-symmetric friend relationships.
    
    Can test result in 'asymmetric_friendship.json'
"""

mr = MapReduce.MapReduce()


def mapper(friends):
    # friends: [personA, personB]
    key=tuple(sorted(friends))
    values=friends[0]
    mr.emit_intermediate(key, values)

def reducer(key, values):
    total=0
    for v in values:
        total=total+1
    if total!=2:
        mr.emit((key))
        mr.emit((key[1],key[0]))
        

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
