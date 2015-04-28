#!/usr/bin/python

import boto.dynamodb
import subprocess

conn = boto.dynamodb.connect_to_region('<your-region>')
table = conn.get_table('<your-dynamo-table>')

cat = subprocess.Popen(["hadoop", "fs", "-cat", "/grep-output/part-r-00000"], stdout=subprocess.PIPE)

item_phrase = ""
item_count = ""

tup_bag = []
for line in cat.stdout:
    split_line = line.strip('\n').split('\t')
    #In case of empty lines
    if split_line[0] == "":
        break
    item_count = split_line[0]
    item_phrase = split_line[1]
    item_data = { 'count' : int(item_count)}
    item = table.new_item(hash_key=item_phrase,attrs=item_data)
    item.save()
