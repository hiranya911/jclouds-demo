#!/usr/bin/python

import sys

input = open(sys.argv[1])
lines = input.readlines()
sum = 0.0
count = 0
min = None
max = None
for line in lines:
  value = float(line)
  if min is None or value < min:
    min = value
  if max is None or value > max:
    max = value
    
  sum += value
  count += 1

print 'No. of inputs: ', count
print 'Min: ', min
print 'Max: ', max
print 'Average: ', sum / count 
