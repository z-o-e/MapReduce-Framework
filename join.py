import MapReduce
import sys

"""
    Perfom "join" function similar as in SQL query of two input tables where 
    'line_item' records(17 elements) and 'order' records(10 elements) are combined 
    in terms of 'order_id'(index1)
    
    Can test result with data in 'records.json'
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # id: 'order_id' included in either 'line_item' or 'order' record
    id = record[1]
    mr.emit_intermediate(id, record)


def reducer(id, record):
    # id: 'order_id'
    # item: record, where item[0] is the category indicator(whether 'line_item' or 'order')
    list1=[]
    list2=[]
    for item in record:
        if(item[0]=='order'):
            list1=item
        else:
            list2=item
            mr.emit(list1+list2)


if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
