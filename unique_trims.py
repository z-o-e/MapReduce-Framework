import MapReduce
import sys

"""
    Within a set of key-value pairs where each key is sequence id and each value
    is a string of nucleotides, remove the last 10 characters from each string of 
    nucleotides, then remove any duplicates generated.
    
    Can test result with data in 'dna.json'
"""


mr = MapReduce.MapReduce()


def mapper(dna):
    # key: nucleotide
    key = dna[1][0:-10]
    value= 'rubbish'
    mr.emit_intermediate(key, value)


def reducer(key, list_of_values):
    # key:sequence id and trimmed nucleotides
    mr.emit((key))


if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
