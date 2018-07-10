# coding=utf-8
# !/usr/bin/env python

import itertools
import sys

stdin_generator = (line for line in sys.stdin if line)
for lines in stdin_generator:
    #txt = lines.strip().split(' ')
    if len(lines)>2:
        print(lines.strip())
# for key, values in itertools.groupby(stdin_generator, key=lambda x: x.split('\t')[0]):
#     # value_sum = sum((int(i.split('\t')[1]) for i in values))
#     print ('%s\t%d' % (key, values))
