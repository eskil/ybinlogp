#!/opt/local/bin/python2.6

import ybinlogp
import time

def print_queries(queries):
	for event, query in queries:
		print ('%s' % time.ctime (event.timestamp))
		print ('server: %d' % event.server_id)
		print ('qtime: %d' % query.query_time)
		print ('database: %s' % query.database)
		print ('error: %d ' % query.error_code)
		print ('%s\n' % query.statement)

if __name__ == "__main__":
	binlog = ybinlogp.binlog ("mysql/mysql-bin.000001", 0, 0)
	queries = []
	for entry in binlog:
		if entry.query:
			queries.append((entry.event, entry.query,));
	# This here del and adding to a list crap is just to excersize the
	# memory crap to look for things that aren't properly managed.
	del binlog
	print_queries(queries)
