#!/opt/local/bin/python2.6
import os
import sys
import datetime
import ybinlogp

if len(sys.argv) == 1:
	print ('Usage: %s <binlog>'  % (sys.argv[0]))
	os._exit(1)

log = open(sys.argv[1])
# When calling from Python, you're responsible for seeking the fd to
# the right offset, aka 4 bytes into the file to skip the magic bytes.
log.seek(4)
binlog = ybinlogp.binlog(log.fileno())

# Binlog is iterable...
for entry in binlog:
	# All entries have member event, and the type_code indicates the
	# type of event.  Eg. entry.event.type_code == 2 means it's a
	# query. Then you can access entry.query. For a list of type
	# codes, see ybinlogp.cc(enum e_event_types).
	
	# You can also switch-case by checking entry.query, entry.rotate
	# and see which is non-null.

	# For a list of entry-entries, see ybinlogp.cc

	print ('event type %d at %s' % (entry.event.type_code, datetime.datetime.fromtimestamp(entry.event.timestamp)))

	if entry.query:
		print ('query %s, %s' % (entry.query.database, entry.query.statement))
	elif entry.rotate:
		print ('rotate %d, %s' % (entry.rotate.next_position, entry.rotate.next_file))
	elif entry.xid:
		print ('xid %d' % (entry.xid.id))
	elif entry.rand:
		print ('rand %d %d' % (entry.rand.seed_1, entry.rand.seed_2))
	elif entry.format_description:
		print ('description %d %d %s' % (entry.format_description.format_version, entry.format_description.create_timestamp, entry.format_description.server_version))
	elif entry.intvar:
		print ('intvar %d = %d' % (entry.intvar.type, entry.intvar.value))


	# For a list of attributes on on the various entries, see
	# ybinlogp.cc. Patch for adding docstring entries to the
	# boost.python bindings welcome.
