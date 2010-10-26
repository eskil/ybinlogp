#!/opt/local/bin/python2.6
import os
import ybinlogp

log = os.open('mysql/mysql-bin.000001', os.O_RDONLY)
os.read(log, 4)
binlog = ybinlogp.binlog(log)

for entry in binlog:
	if entry.query:
		print ('query %s, %s' % (entry.query.database, entry.query.statement))
	elif entry.rotate:
		print ('rotate %d, %s' % (entry.rotate.next_position, entry.rotate.next_file))
	elif entry.xid:
		print ('xid %d' % (entry.xid.id))
	elif entry.rand:
		print ('rand %d %d' % (entry.rand.seed_1, entry.rand.seed_2))
	elif entry.format_description:
		print ('description %d %d %s' % (entry.format_description.format_version, entry.format_description.timestamp, entry.format_description.server_version))
	elif entry.intvar:
		print ('intvar %d = %d' % (entry.intvar.type, entry.intvar.value))

