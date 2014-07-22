#coding=utf-8 
import sys
import os
import sqlite3

def parse_record(input, records):
	for line in input:
		cols = line.split('||')
		if(len(cols) < 5):
			continue
		col_index = 0
		record = []
		for col in cols:
			col = col.strip()
			if col_index == 0:
				if len(col) == 0:
					continue
				try:
					nid = int(col)
				except ValueError:
					break
				else:
					col_index = col_index + 1
					record.append(nid)
			else:
				col_index = col_index + 1
				record.append(col)
				if (col_index == 8):
					records.append(record)
					break
	print "parse done! size: ", len(records)

def import_db(records):
	con = sqlite3.connect(":memory:")
	con.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
	con.execute("create table dogfood (nid integer primary key, name varchar(20), regtime date, orderid varchar(10), ptype varchar(10), ordertype varchar(20), des text, result text)")
	for record in records:
    		con.execute("insert into dogfood values (?,?,?,?,?,?,?,?)", record)
	con.commit()
	print "import done!"
	return con

def query_db(con, query):
	cu = con.cursor()
	cu.execute(query)
	for record in cu.fetchall():
		for col in record:
			print col, 
		print
	print

if __name__ == '__main__':

	if(len(sys.argv) < 2):
		print "need file name as argument."
		quit()
	try:
		input = open(sys.argv[1], 'r') 
	except IOError:
		print "open file failed."
		quit()
	else:
		records = []
		parse_record(input, records)
		input.close()
		con = import_db(records)
		print "all stat:"
		query_count_per_name = "select name, count(*) as total from dogfood group by name order by total desc"
		query_db(con, query_count_per_name)
		query_count_per_ordertype = "select ordertype, count(*) as total from dogfood group by ordertype order by total desc"
		query_db(con, query_count_per_ordertype)
		print "last week stat:"
		query_count_per_name_weekly = "select name, count(*) as total from dogfood where regtime> date('now', '-7 day') group by name order by total desc"
		query_db(con, query_count_per_name_weekly)
		query_count_per_ordertype_weekly = "select ordertype, count(*) as total from dogfood where regtime > date('now', '-7 day') group by ordertype order by total desc"
		query_db(con, query_count_per_ordertype_weekly)
