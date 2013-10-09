import sys
from random import randint

if __name__ == '__main__':
    count = int(sys.argv[1])
    while count: 
	if count % 2 == 1:
          print randint(0, 10000)
	else:
	  print randint(20000, 30000)
	count -= 1
