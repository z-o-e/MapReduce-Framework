import MapReduce
import sys

"""
    A MapReduce algorithm to compute matrix multiplication: A x B,
    where A and B are in a sparse matrix format.
    
    Can test with data in 'multiply.json'
"""

mr = MapReduce.MapReduce()


def mapper(matrices):
    # matices: [matrix_id, row_id, col_id, value]
    if matrices[0]=='a':
        for j in range(5):
            key = tuple([matrices[1],j])
            mr.emit_intermediate(key, matrices)
    else:
        for i in range(5):
            key =tuple([i,matrices[2]])
            mr.emit_intermediate(key, matrices)
    
def reducer(key, values):
    value=0
    for i in range(5):
        temp1=0
        temp2=0
        for v in values:
            if v[0]=='a' and v[2]==i:
                temp1=v[3]
            elif v[0]=='b'and v[1]==i:
                temp2=v[3] 
                value=value+temp1*temp2
    mr.emit((key[0],key[1],value))
        
            
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
