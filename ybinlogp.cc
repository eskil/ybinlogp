/*
 * ybinlogp: A mysql binary log parser and query tool
 *
 * (C) 2010 Yelp, Inc.
 */

#define _XOPEN_SOURCE 600
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <assert.h>
#include <alloca.h>

#include "ybinlogp.hh"

#undef _GNU_SOURCE
#include <boost/python.hpp>
#include <boost/format.hpp>
#include <boost/bind.hpp>
#include <iostream>

// Python bindings... all of it... seriously */

namespace {
    std::string entry_str (const yelp::binlog::entry &evbuf) {
        std::ostringstream os;
        os << evbuf;
        return os.str ();
    }

    // Junk to overcome const overloading, and this didn't work ;
    // const event_buffer* (yelp::binlog::entry::*const_get_entry_buffer) () const = &yelp::binlog::entry::get_buffer;
    // ... go figure...
    const event_buffer* const_get_entry_buffer (const yelp::binlog::entry &entry) {
        return entry.get_buffer ();
    }

    // template to create a specifc entry type from an entry, checks
    // the type_code and returns a nice python None if it's not right.
    template<typename T, int C>
    T* new_from_entry (const yelp::binlog::entry &entry) {
        if (entry.get_buffer()->type_code != C) {
            return NULL;
        }
        return new T (*entry.get_buffer ());
    }
}


BOOST_PYTHON_MODULE(ybinlogp) {
    using namespace boost::python;

    scope().attr ("__doc__") = "Hey I'm a docstring for the binlog parser woot!";

    class_<event_buffer> ("event", "A MySQL event")
        .def_readonly ("timestamp", &event_buffer::timestamp)
        .def_readonly ("server_id", &event_buffer::server_id)
        ;
    class_<yelp::binlog::format_description_entry> ("format_description", "A MySQL format description", no_init)
        .def_readonly ("format_version", &yelp::binlog::format_description_entry::format_version)
        .def_readonly ("timestamp", &yelp::binlog::format_description_entry::timestamp)
        .def_readonly ("server_version", &yelp::binlog::format_description_entry::server_version)
        ;
    class_<yelp::binlog::query_entry> ("query", "A MySQL query event", no_init)
        .def_readonly ("thread_id", &yelp::binlog::query_entry::thread_id)
        .def_readonly ("query_time", &yelp::binlog::query_entry::query_time)
        .def_readonly ("error_code", &yelp::binlog::query_entry::error_code)
        .def_readonly ("database", &yelp::binlog::query_entry::database)
        .def_readonly ("statement", &yelp::binlog::query_entry::statement)
        ;
    class_<yelp::binlog::rand_entry> ("rand", "A MySQL rand event", no_init)
        .def_readonly ("seed_1", &yelp::binlog::rand_entry::seed_1)
        .def_readonly ("seed_2", &yelp::binlog::rand_entry::seed_2)
        ;
    class_<yelp::binlog::intvar_entry> ("intvar", "A MySQL intvar event", no_init)
        .def_readonly ("type", &yelp::binlog::intvar_entry::type)
        .def_readonly ("value", &yelp::binlog::intvar_entry::value)
        ;
    class_<yelp::binlog::rotate_entry> ("rotate", "A MySQL rotate binlog entry", no_init)
        .def_readonly ("next_position", &yelp::binlog::rotate_entry::next_position)
        .def_readonly ("next_file", &yelp::binlog::rotate_entry::next_file)
        ;
    class_<yelp::binlog::xid_entry> ("xid", "A MySQL xid binlog entry", no_init)
        .def_readonly ("id", &yelp::binlog::xid_entry::id)
        ;
    class_<yelp::binlog::entry> ("entry", "A MySQL query event")
        .def ("__str__", &entry_str)
        .def ("__repr__", &entry_str)
        .add_property ("event", make_function (&const_get_entry_buffer, return_value_policy<reference_existing_object> ()))
        .add_property ("format_description", make_function (new_from_entry<yelp::binlog::format_description_entry, 15>,
                                                            return_value_policy<manage_new_object> ()))
        .add_property ("query", make_function (new_from_entry<yelp::binlog::query_entry, 2>,
                                               return_value_policy<manage_new_object> ()))
        .add_property ("rand", make_function (new_from_entry<yelp::binlog::rand_entry, 13>,
                                              return_value_policy<manage_new_object> ()))
        .add_property ("intvar", make_function (new_from_entry<yelp::binlog::intvar_entry, 5>,
                                                return_value_policy<manage_new_object> ()))
        .add_property ("rotate", make_function (new_from_entry<yelp::binlog::rotate_entry, 4>,
                                                return_value_policy<manage_new_object> ()))
        .add_property ("xid", make_function (new_from_entry<yelp::binlog::xid_entry, 16>,
                                             return_value_policy<manage_new_object> ()))
        ;
    class_<yelp::binlog::iterator> ("binlog.iterator", "This is MySQL binlog iterator")
        ;
    class_<yelp::binlog> ("binlog", "This is MySQL binlog file parser", init<int> ())
        //.def ("__iter__", iterator<yelp::binlog> ())
        .def ("__iter__", iterator<yelp::binlog> ())
        ;
}

namespace {
    // how many bytes to seek ahead looking for a record
    static const uint32_t MAX_RETRIES = 102400;

    template<typename T1, typename T2>
    inline T1 get_bit(T1 x, T2 bit) { return !!(x & 1 << (bit-1)); }

    // binlog parameters
    static const uint16_t MIN_TYPE_CODE = 0;
    static const uint16_t MAX_TYPE_CODE = 27;
    static const uint16_t MIN_EVENT_LENGTH = 19;
    // Can't see why you'd have events >10MB.
    static const uint32_t MAX_EVENT_LENGTH = 10485760;
    // 0 <= server_id  <= 2**31
    static const uint32_t MAX_SERVER_ID = 4294967295;

    // Used in main to toggle dumping query details or not.
    int q_mode = 0;

    const char* event_types[27] = {
        "UNKNOWN_EVENT",			// 0
        "START_EVENT_V3",			// 1
        "QUERY_EVENT",				// 2
        "STOP_EVENT",				// 3
        "ROTATE_EVENT",				// 4
        "INTVAR_EVENT",				// 5
        "LOAD_EVENT",				// 6
        "SLAVE_EVENT",				// 7
        "CREATE_FILE_EVENT",			// 8
        "APPEND_BLOCK_EVENT",			// 9
        "EXEC_LOAD_EVENT",			// 10
        "DELETE_FILE_EVENT",			// 11
        "NEW_LOAD_EVENT",			// 12
        "RAND_EVENT",				// 13
        "USER_VAR_EVENT",			// 14
        "FORMAT_DESCRIPTION_EVENT",		// 15
        "XID_EVENT",				// 16
        "BEGIN_LOAD_QUERY_EVENT",		// 17
        "EXECUTE_LOAD_QUERY_EVENT",		// 18
        "TABLE_MAP_EVENT",			// 19
        "PRE_GA_WRITE_ROWS_EVENT",		// 20
        "PRE_GA_DELETE_ROWS_EVENT",		// 21
        "WRITE_ROWS_EVENT",			// 22
        "UPDATE_ROWS_EVENT",			// 23
        "DELETE_ROWS_EVENT",			// 24
        "INCIDENT_EVENT",			// 25
        "HEARTBEAT_LOG_EVENT"			// 26
    };

    /*
      // Why aren't these used
    const char* variable_types[10] = {
        "Q_FLAGS2_CODE",			// 0
        "Q_SQL_MODE_CODE",			// 1
        "Q_CATALOG_CODE",			// 2
        "Q_AUTO_INCREMENT",			// 3
        "Q_CHARSET_CODE",			// 4
        "Q_TIME_ZONE_CODE",			// 5
        "Q_CATALOG_NZ_CODE",			// 6
        "Q_LC_TIME_NAMES_CODE",			// 7
        "Q_CHARSET_DATABASE_CODE",		// 8
        "Q_TABLE_MAP_FOR_UPDATE_CODE",		// 9
    };
    */

    const char* intvar_types[3] = {
        "",
        "LAST_INSERT_ID_EVENT",			// 1
        "INSERT_ID_EVENT",			// 2
    };

    const char* flags[16] = {
        "LOG_EVENT_BINLOG_IN_USE",		// 0x01
        "LOG_EVENT_FORCED_ROTATE",		// 0x02 (deprecated)
        "LOG_EVENT_THREAD_SPECIFIC",		// 0x04
        "LOG_EVENT_SUPPRESS_USE",		// 0x08
        "LOG_EVENT_UPDATE_TABLE_MAP_VERSION",	// 0x10
        "LOG_EVENT_ARTIFICIAL",			// 0x20
        "LOG_EVENT_RELAY_LOG",			// 0x40
        "",
        "",
    };

    void usage (void) {
        fprintf (stderr, "Usage: ybinlogp [mode] logfile [mode-args]\n");
        fprintf (stderr, "\n");
        fprintf (stderr, "ybinlogp supports several different modes:\n");
        fprintf (stderr, "\t-o Find the first event after the given offset\n");
        fprintf (stderr, "\t\tybinlogp -o offset logfile\n");
        fprintf (stderr, "\t-t Find the event closest to the given unix time\n");
        fprintf (stderr, "\t\tybinlogp -t timestamp logfile\n");
        fprintf (stderr, "\t-a When used with one of the above, print N items after the first one\n");
        fprintf (stderr, "\t\tAccepts either an integer or the text 'all'\n");
        fprintf (stderr, "\t\tybinlogp -a N -t timestamp logfile\n");
        fprintf (stderr, "\t-q Be slightly quieter when printing (don't print statement contents\n");
        fprintf (stderr, "\t-Q Be much quieter (only print offset, timestamp, and type code)\n");
    }


    void init_event (struct event_buffer *evbuf) {
        memset (evbuf, 0, sizeof (struct event_buffer));
    }


    void dispose_event (struct event_buffer *evbuf) {
        if (evbuf == NULL) {
            return;
        }
        delete[] evbuf->heaped;
#if DEBUG
        evbuf->heaped = (char*)0xdeadbeef;
        evbuf->data = (char*)0xdeadbeef;
#endif
        free (evbuf);
    }


    void reset_event (struct event_buffer *evbuf) {
#if DEBUG
        fprintf (stderr, "Resetting event\n");
#endif
        delete[] evbuf->heaped;
        evbuf->heaped = NULL;
        evbuf->data = NULL;
        init_event (evbuf);
    }


    // dest and source must not overlap
    int copy_event (event_buffer *dest, const event_buffer *source) {
#if DEBUG
        fprintf(stderr, "About to copy 0x%p to 0x%p\n", source, dest);
#endif
        init_event (dest);
        memcpy (dest, source, sizeof (struct event_buffer));
        if (source->data != 0) {
#if DEBUG
            fprintf (stderr, "newing %lu bytes for the target\n", source->length - EVENT_HEADER_SIZE);
#endif
            if (source->length - EVENT_HEADER_SIZE > sizeof (dest->payload)) {
                if ((dest->heaped = new char[source->length - EVENT_HEADER_SIZE]) == NULL) {
                    perror ("new:");
                    return -1;
                }
                dest->data = dest->heaped;
            } else {
                dest->heaped = NULL;
                dest->data = (char*)&(dest->payload);
            }
#if DEBUG
            fprintf (stderr, "copying extra data from 0x%p to 0x%p\n", source->data, dest->data);
#endif
            memcpy (dest->data, source->data, source->length - EVENT_HEADER_SIZE);
        }
        return 0;
    }
}


namespace yelp {
    // This thing is a tad slow compared to jbrown's original fprintf version, but it's ostream compatible...
    std::ostream& operator<< (std::ostream &os, const event_buffer &ev) {
        using namespace boost;

        const time_t t = ev.timestamp;
        os << "BYTE OFFSET " << (long long)ev.offset << "\n"
           << "------------------------\n"
           << "timestamp:          " << ev.timestamp << " = " << ctime(&t)
           << "type_code:          " << event_types[ev.type_code] << "\n";
        if (q_mode > 1) {
            return os;
        }
        os << "server id:          " << ev.server_id << "\n"
           << "length:             " << ev.length << "\n"
           << "next pos:           " << (unsigned long long)ev.next_position << "\n"
           << "flags:              ";
        for (int i=16; i > 0; --i) {
            os << (format ("%hhd") % get_bit (ev.flags, i));
        }
        os << "\n";
        for (int i=16; i > 0; --i) {
            if (get_bit (ev.flags, i)) {
                os << "                        " << flags[i-1] << "\n";
            }
        }
        if (ev.data == NULL) {
            return os;
        }
        switch (ev.type_code) {
        case 2: {
            // QUERY_EVENT
            struct query_event_buffer *q = (struct query_event_buffer*)(ev.data);
            char* db_name = query_event_db_name((&ev));
            size_t statement_len = query_event_statement_len((&ev));
            char *statement = (char*)alloca (statement_len + 1);
            memcpy (statement, (const char*)query_event_statement((&ev)), statement_len);
            statement[statement_len] = '\0';
            if (statement == NULL) {
                std::cerr << "strndupa: " << ::strerror (errno) << "\n";
                return os;
            }
            os << "thread id:          " << q->thread_id << "\n"
               << "query time (s):     " << q->query_time << "\n";
            if (q->error_code == 0) {
                os << "error code:         " << q->error_code << "\n";
            } else {
                os << "ERROR CODE:         " << q->error_code << "\n";
            }
            os << "status var length:  " << q->status_var_len << "\n"
               << "db_name:            " << db_name << "\n"
               << "statement length:   " << statement_len << "\n";
            if (q_mode > 0) {
                os << "statement:          " << statement << "\n";
            }
            break;
        }
        case 4: {
            struct rotate_event_buffer *r = (struct rotate_event_buffer*)ev.data;
            size_t len = rotate_event_file_name_len((&ev));
            // ghetto inlined strndupa...
            char *file_name = (char*)alloca (len + 1);
            memcpy (file_name, (const char*)rotate_event_file_name((&ev)), len);
            file_name[len] = '\0';
            os << "next log position:  " << (unsigned long long)r->next_position << "\n"
               << "next file name:     " << file_name << "\n";
            break;
        }
        case 5: {
            struct intvar_event_buffer *i = (struct intvar_event_buffer*)ev.data;
            os << "variable type:      " << intvar_types[i->type] << "\n"
               << "value: 	            " << (unsigned long long)i->value << "\n";
            break;
        }
        case 13: {
            struct rand_event_buffer *r = (struct rand_event_buffer*)ev.data;
            os << "seed 1:	            " << (unsigned long long)r->seed_1 << "\n"
           << "seed 2:	            " << (unsigned long long)r->seed_2 << "\n";
            break;
        }
        case 15: {
        // FORMAT_DESCRIPTION_EVENT
            struct format_description_event_buffer *f = (struct format_description_event_buffer*)ev.data;
            os << "binlog version:     " << f->format_version << "\n"
               << "server version:     " << f->server_version << "\n"
               << "variable length:    " << format_description_event_data_len((&ev)) << "\n";
            break;
        }
        case 16: {
            // XID_EVENT
            struct xid_event_buffer *x = (struct xid_event_buffer*)ev.data;
            os << "xid id:             " << (unsigned long long)x->id << "\n";
            break;
        }
        }
        return os;
    }

    binlog::binlog (const std::string &filename, off64_t starting_offset, time_t starting_time)
        : m_fd (-1), m_owns_file (true), m_stbuf (new struct stat), m_evbuf (NULL),
          m_min_timestamp (0), m_max_timestamp (::time(NULL))
    {
        if (stat (filename.c_str (), m_stbuf)) {
            throw std::runtime_error (std::string ("stat: ") + ::strerror (errno));
        }

        int oflags = O_RDONLY;
#ifndef DARWIN
        oflags |= O_LARGEFILE;
#endif /* DARWIN */
        if ((m_fd = ::open (filename.c_str (), oflags)) <= 0) {
            throw std::runtime_error (std::string ("open: ") + ::strerror (errno));
        }

        m_evbuf = (struct event_buffer*)malloc (sizeof (struct event_buffer));
        init_event (m_evbuf);
        if (read_fde (m_evbuf) < 0) {
            throw std::runtime_error (std::string ("read_fde: ") + ::strerror (errno));
        }
        m_min_timestamp = m_evbuf->timestamp;

        off64_t offset = 0;
        if (starting_time > 0) {
            offset = nearest_time (starting_time, m_evbuf);
        } else if (starting_offset) {
            offset = nearest_offset (starting_offset, m_evbuf, 1);
        }

        if (offset < 0) {
            throw std::runtime_error (std::string ("no records found: ") + ::strerror (errno));
        }
    }


    binlog::binlog (int fd)
        : m_fd (fd), m_owns_file (false), m_stbuf (NULL), m_evbuf (NULL),
          m_min_timestamp (0), m_max_timestamp (::time(NULL))
    {
        m_evbuf = (struct event_buffer*)malloc (sizeof (struct event_buffer));
        init_event (m_evbuf);
        if (read_fde (m_evbuf) < 0) {
            throw std::runtime_error (std::string ("read_fde: ") + ::strerror (errno));
        }
        m_min_timestamp = m_evbuf->timestamp;
    }


    binlog::~binlog () {
        dispose_event (m_evbuf);
        if (m_owns_file) {
            ::close (m_fd);
        }
        delete m_stbuf;
    }


    /**
     * Read the FDE and set the server-id
     **/
    int binlog::read_fde (struct event_buffer *evbuf) {
        if (read_event (evbuf, 4) < 0) {
            return -1;
        }
#if DEBUG
        std::cout << *evbuf;
#endif
        struct format_description_event_buffer *f = (struct format_description_event_buffer*) evbuf->data;
        if (f->format_version != BINLOG_VERSION) {
            errno = EINVAL;
            return -1;
        }
        return 0;
    }


    int binlog::read_event (struct event_buffer *evbuf, off64_t offset) {
        ssize_t amt_read;
        if (m_owns_file && (::lseek (m_fd, offset, SEEK_SET) < 0)) {
            throw std::runtime_error (std::string ("lseek:") + ::strerror (errno));
        }
        amt_read = ::read (m_fd, (void*)evbuf, EVENT_HEADER_SIZE);
        evbuf->offset = offset;
        evbuf->data = NULL;
        if (amt_read < 0) {
            throw std::runtime_error (std::string ("read: ") + ::strerror (errno));
        } else if ((size_t)amt_read != EVENT_HEADER_SIZE) {
            return -1;
        }
        if (check_event (evbuf)) {
#if DEBUG
            fprintf (stdout, "newing %lu bytes\n", evbuf->length - EVENT_HEADER_SIZE);
#endif
            if (evbuf->length - EVENT_HEADER_SIZE > sizeof (evbuf->payload)) {
                evbuf->heaped = new char[evbuf->length - EVENT_HEADER_SIZE];
                evbuf->data = evbuf->heaped;
            } else {
                evbuf->heaped = NULL;
                evbuf->data = (char*)&(evbuf->payload);
            }
#if DEBUG
            fprintf(stderr, "newed %lu bytes at 0x%p for a %s\n", evbuf->length - EVENT_HEADER_SIZE, evbuf->data, event_types[evbuf->type_code]);
#endif
            if (read (m_fd, evbuf->data, evbuf->length - EVENT_HEADER_SIZE) < 0) {
                throw std::runtime_error (std::string ("read extra (short): ") + ::strerror (errno));
            }
        }
        return 0;
    }


    int binlog::check_event (struct event_buffer *evbuf) {
        if (evbuf->server_id < MAX_SERVER_ID &&
            evbuf->type_code > MIN_TYPE_CODE &&
            evbuf->type_code < MAX_TYPE_CODE &&
            evbuf->length > MIN_EVENT_LENGTH &&
            evbuf->length < MAX_EVENT_LENGTH &&
            evbuf->timestamp >= m_min_timestamp &&
            evbuf->timestamp <= m_max_timestamp) {
            return 1;
        } else {
            return 0;
        }
    }


    off64_t binlog::next_after (struct event_buffer *evbuf) {
        /* Can't actually use next_position, because it will vary between
         * messages that are from master and messages that are from slave.
         * Usually, only the FDE is from the slave. But, still...
         */
        return evbuf->offset + evbuf->length;
    }


    off64_t binlog::nearest_offset (off64_t starting_offset, struct event_buffer *outbuf, int direction) {
        unsigned int num_increments = 0;
        off64_t offset;
        struct event_buffer *evbuf = (struct event_buffer*)malloc (sizeof(struct event_buffer));
        init_event (evbuf);
        offset = starting_offset;
#if DEBUG
        fprintf (stderr, "In nearest offset mode, got fd=%d, starting_offset=%llu\n", fd, (long long)starting_offset);
#endif
        while (num_increments < MAX_RETRIES && offset >= 0 && offset <= m_stbuf->st_size - (off64_t)EVENT_HEADER_SIZE) {
            reset_event (evbuf);
            if (read_event (evbuf, offset) < 0) {
                dispose_event (evbuf);
                return -1;
            }
            if (check_event (evbuf)) {
                if (outbuf != NULL) {
                    reset_event (outbuf);
                    copy_event (outbuf, evbuf);
                }
                dispose_event (evbuf);
                return offset;
            } else {
                offset += direction;
                ++num_increments;
            }
        }
        dispose_event (evbuf);
#if DEBUG
        fprintf (stderr, "Unable to find anything (offset=%llu)\n",(long long) offset);
#endif
        return -2;
    }


    int binlog::nearest_time (time_t target, struct event_buffer *outbuf) {
        off64_t file_size = m_stbuf->st_size;
        struct event_buffer *evbuf = (struct event_buffer*)malloc (sizeof(struct event_buffer));
        init_event(evbuf);
        off64_t offset = file_size / 2;
        off64_t next_increment = file_size / 4;
        int directionality = 1;
        off64_t found, last_found = 0;
        while (next_increment > 2) {
            long long delta;
            reset_event (evbuf);
            found = nearest_offset (offset, evbuf, directionality);
            if (found == -1) {
                return found;
            } else if (found == -2) {
                fprintf (stderr, "Ran off the end of the file, probably going to have a bad match\n");
                last_found = found;
                break;
            }
            last_found = found;
            delta = (evbuf->timestamp - target);
            if (delta > 0) {
                directionality = -1;
            } else if (delta < 0) {
                directionality = 1;
            }
#if DEBUG
            fprintf (stderr, "delta=%lld at %llu, directionality=%d, next_increment=%lld\n", (long long)delta, (unsigned long long)found, directionality, (long long)next_increment);
#endif
            if (delta == 0) {
                break;
            }
            if (directionality == -1) {
                offset += (next_increment  * directionality);
            } else {
                offset += (next_increment * directionality);
            }
            next_increment /= 2;
        }
        if (outbuf) {
            copy_event (outbuf, evbuf);
        }
        dispose_event (evbuf);
        return last_found;
    }


    binlog::format_description_entry::format_description_entry (const struct event_buffer &evbuf)
        : format_version (0), timestamp (0)
    {
        if (evbuf.type_code != 15) {
            throw std::invalid_argument ((boost::format ("event_buffer has type_code %d, not valid for format_description_entry") % evbuf.type_code).str ());
        }
        struct format_description_event_buffer *f = (struct format_description_event_buffer*)evbuf.data;
        format_version = f->format_version;
        timestamp = f->timestamp;
        server_version = f->server_version;
    }


    binlog::query_entry::query_entry (const struct event_buffer &evbuf)
        : thread_id (0), query_time (0), error_code (0)
    {
        if (evbuf.type_code != 2) {
            throw std::invalid_argument ((boost::format ("event_buffer has type_code %d, not valid for query_entry") % evbuf.type_code).str ());
        }
        struct query_event_buffer *q = (struct query_event_buffer*)(evbuf.data);
        thread_id = q->thread_id;
        query_time = q->query_time;
        error_code = q->error_code;
        database = query_event_db_name ((&evbuf));
        size_t statement_len = query_event_statement_len ((&evbuf));
        statement = std::string ((const char*)query_event_statement ((&evbuf)), statement_len);
    }


    binlog::rand_entry::rand_entry (const struct event_buffer &evbuf)
        : seed_1 (0), seed_2 (0)
    {
        if (evbuf.type_code != 13) {
            throw std::invalid_argument ((boost::format ("event_buffer has type_code %d, not valid for rand_entry") % evbuf.type_code).str ());
        }
        struct rand_event_buffer *r = (struct rand_event_buffer*)evbuf.data;
        seed_1 = r->seed_1;
        seed_2 = r->seed_2;
    }


    binlog::intvar_entry::intvar_entry (const struct event_buffer &evbuf)
        : type (0), value (0)
    {
        if (evbuf.type_code != 5) {
            throw std::invalid_argument ((boost::format ("event_buffer has type_code %d, not valid for intvar_entry") % evbuf.type_code).str ());
        }
        struct intvar_event_buffer *i = (struct intvar_event_buffer*)(evbuf.data);
        type = i->type;
        value = i->value;
    }


    binlog::rotate_entry::rotate_entry (const struct event_buffer &evbuf)
        : next_position (0)
    {
        if (evbuf.type_code != 4) {
            throw std::invalid_argument ((boost::format ("event_buffer has type_code %d, not valid for rotate_entry") % evbuf.type_code).str ());
        }
        struct rotate_event_buffer *r = (struct rotate_event_buffer*)evbuf.data;
        next_position = r->next_position;
        size_t len = rotate_event_file_name_len((&evbuf));
        next_file = std::string ((const char*)rotate_event_file_name((&evbuf)), len);
    }


    binlog::xid_entry::xid_entry (const struct event_buffer &evbuf)
        : id (0)
    {
        if (evbuf.type_code != 16) {
            throw std::invalid_argument ((boost::format ("event_buffer has type_code %d, not valid for id_entry") % evbuf.type_code).str ());
        }
        struct xid_event_buffer *x = (struct xid_event_buffer*)evbuf.data;
        id = x->id;
    }


    binlog::entry::entry () {
        init_event (&m_buffer);
    }


    binlog::entry::entry (const entry &rhs) {
        copy_event (&m_buffer, &rhs.m_buffer);
    }


    binlog::entry::entry (const event_buffer *evbuf) {
        if (evbuf) {
            copy_event (&m_buffer, evbuf);
        } else {
            init_event (&m_buffer);
        }
    }


    binlog::entry& yelp::binlog::entry::operator= (const entry &rhs) {
        reset_event (&m_buffer);
        copy_event (&m_buffer, &rhs.m_buffer);
        return *this;
    }


    bool binlog::entry::operator== (const entry &rhs) const {
        return memcmp (&m_buffer, &rhs.m_buffer, sizeof (event_buffer)) == 0;
    }


    binlog::entry::~entry () {
        delete[] m_buffer.heaped;
    }


    binlog::iterator::iterator (const iterator &rhs)
        : m_entry (rhs.m_entry), m_binlog (rhs.m_binlog)
    { }


    /*
    binlog::iterator::iterator& binlog::iterator::operator= (const iterator& rhs) {
        iterator tmp (rhs);
        std::swap (*this, tmp);
        return *this;
    }
    */


    binlog::iterator& binlog::iterator::operator++ () {
        off64_t offset = m_binlog->next_after (m_entry.get_buffer ());
        m_entry = entry ();
        if (m_binlog->read_event (m_entry.get_buffer (), offset) < 0) {
            m_entry = entry ();
        }
        return *this;
    }
}


int main (int argc, char **argv) {
	int opt;
	time_t target_time = 0;
	off64_t starting_offset = 0;
	int show_all = 0;
	int num_to_show = 1;
	int t_mode = 0;
	int o_mode = 1;

	/* Parse args */
	while ((opt = getopt(argc, argv, "t:o:a:qQ")) != -1) {
		switch (opt) {
        case 't':		/* Time mode */
            target_time = atol(optarg);
            t_mode = 1;
            o_mode = 0;
            break;
        case 'o':		/* Offset mode */
            starting_offset = atoll(optarg);
            t_mode  = 0;
            o_mode = 1;
            break;
        case 'a':
            if (strncmp(optarg, "all", 3) == 0) {
                num_to_show = 2;
                show_all = 1;
                break;
            }
            num_to_show = atoi(optarg);
            if (num_to_show < 1)
                num_to_show = 1;
            break;
        case 'q':
            q_mode = 1;
            break;
        case 'Q':
            q_mode = 2;
            break;
        case '?':
            fprintf(stderr, "Unknown argument %c\n", optopt);
            usage();
            return 1;
            break;
        default:
            usage();
            return 1;
		}
	}
	if (optind >= argc) {
		usage();
		return 1;
	}

    yelp::binlog binlog (argv[optind], starting_offset, target_time);
    if (show_all) {
        std::copy (binlog.begin (), binlog.end (),
                   std::ostream_iterator<yelp::binlog::entry> (std::cout, "\n"));
    } else {
        yelp::binlog::iterator it = binlog.begin ();
        yelp::binlog::iterator end = binlog.end ();
        for (int n = 0; n < num_to_show && it != end ; ++n) {
            std::cout << *(it++);
        }
    }
    return 0;
}
