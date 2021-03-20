#!/usr/bin/python
import os
import sys
import math

fd1 = open(sys.argv[1], 'r')
fd2 = open(sys.argv[2], 'r')
fd3 = open(sys.argv[3], 'w')

maxdiff = 0
count = 1
num_diff = 0
for line1, line2 in zip(fd1,fd2):
    x = float(line1)
    x2 = float(line2)
    if math.isnan(x) != math.isnan(x2) or math.isinf(x) != math.isinf(x2):
      print('Failure: values do not agree')
      print('Line: ', count, x, x2)
      exit(1)
    diff = x - x2
    fd3.write(str(x - x2) + "\n")
    if(abs(diff) > maxdiff):
        maxdiff = abs(diff)
    if abs(diff) > 0.01:
        print('Failure: matrix profile differs from the ground truth by more than the threshold')
        print('Line: ', count, x, x2)
        num_diff += 1
    count += 1

print("number of values mismatched " + str(num_diff))
print("Max matrix profile difference was " + str(maxdiff))


# python other_files/diff.py other_files/output/scamp.txt other_files/output/gcp.csv other_files/results.txt

# python main.py --time_series /Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomwalk512K.txt
# --subsequence_length=200 --tile_size=10000 --save_main_session True --region us-west1 --runner direct --project
# bscamp11 --temp_location gs://full_bucket11/temp/ --setup_file /Users/kalyesh/PycharmProjects/beam/setup.py >
# other_files/debug.txt
