/*
 * binlogp: A mysql binary log parser and query tool
 *
 * (C) 2010 Yelp, Inc.
 */

#ifndef YELP_YBINLOGP_H
#define YELP_YBINLOGP_H

#include <stdint.h>
#include <sys/types.h>

#include <string>
#include <iterator>
#include <iosfwd>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>

#if defined(DARWIN)
// Darwin doesn't disinguish between 32 and 64 bit offsets, everything in 64bit...
#include <fcntl.h>
typedef off_t off64_t;
#endif

extern "C" {
    const unsigned int BINLOG_VERSION = 4;
    
    // we tack on extra stuff at the end 
    // TODO: this here is "wrong", the FDE determines the real
    // EVENT_HEADER_SIZE in it's header_len field and that's the value
    // that should really be used. It just happens to be 19 now.
    const size_t EVENT_HEADER_SIZE = 19;
    // Room for event data. Semiarbitrarily picked...
    const size_t EVENT_PAYLOAD_SIZE = 32 - EVENT_HEADER_SIZE;
    
    // force byte alignment for all the buffers. They have to be PODs and bmust align with the binlog format
#pragma pack(push)
#pragma pack(1)
    
    struct event_buffer {
        // Event Header
        uint32_t	timestamp;
        uint8_t		type_code;
        uint32_t	server_id;
        uint32_t	length;
        uint32_t	next_position;
        uint16_t	flags;
        // Extra stuff tacked on
        off64_t		offset;
        // Technically we only need 1 of these two pointers, but then we'd just need more checks all over the code
        char*		heaped;
        char*		data;
        // Could use C99 FAM, but that's rarely fun in the long run and we'd
        // need to know the size of the event up front or realloc.
        char		payload[EVENT_PAYLOAD_SIZE];
    };
    
    
    struct format_description_event_buffer {
        uint16_t	format_version;	// ought to be 4
        char		server_version[50];
        uint32_t	create_timestamp;
        uint8_t		header_len;
        // random data
    };
    
#define format_description_event_data(e) (e->data + ((struct format_description_event_buffer*)e->data)->header_length)
#define format_description_event_data_len(e) (((struct format_description_event_buffer*)e->data)->header_len - EVENT_HEADER_SIZE)
    
    struct query_event_buffer {
        uint32_t	thread_id;
        uint32_t	query_time;
        uint8_t		db_name_len;
        uint16_t	error_code;
        uint16_t	status_var_len;
        // status variables (status_var_len)
        // database name    (db_name_len + 1, NUL)
        // statement        (the rest, not NUL)
    };
    
#define query_event_statement(e) (e->data + sizeof(struct query_event_buffer) + ((struct query_event_buffer*)e->data)->status_var_len + ((struct query_event_buffer*)e->data)->db_name_len + 1)
#define query_event_statement_len(e) (e->length - EVENT_HEADER_SIZE - sizeof(struct query_event_buffer) - ((struct query_event_buffer*)e->data)->status_var_len - ((struct query_event_buffer*)e->data)->db_name_len - 1)
#define query_event_db_name(e) (e->data + sizeof(struct query_event_buffer) + ((struct query_event_buffer*)e->data)->status_var_len)
    
    struct rand_event_buffer {
        uint64_t	seed_1;
        uint64_t	seed_2;
    };
    
    struct xid_event_buffer {
        uint64_t	id;
    };
    
    struct intvar_event_buffer {
        uint8_t		type;
        uint64_t	value;
    };
    
    struct rotate_event_buffer {
        uint64_t	next_position;
        // file name of the next file (not NUL)
    };
    
#define rotate_event_file_name(e) (e->data + sizeof (struct rotate_event_buffer))
#define rotate_event_file_name_len(e) (e->length - EVENT_HEADER_SIZE - sizeof (struct rotate_event_buffer))
    
#pragma pack(pop)
}
    

/** */
namespace yelp {

    /** */
    class binlog {
    public:

        /** */
        struct format_description_entry {
            format_description_entry () : format_version (0), create_timestamp (0) { }
            format_description_entry (const struct event_buffer &evbuf);
            bool operator== (const format_description_entry &rhs) const {
                return format_version == rhs.format_version &&
                    create_timestamp == rhs.create_timestamp &&
                    server_version == rhs.server_version;
            }
            uint16_t format_version;
            uint32_t create_timestamp;
            std::string server_version;
        };

        /** */
        struct query_entry {
            query_entry () : thread_id (0), query_time (0), error_code (0) { }
            query_entry (const struct event_buffer &evbuf);
            bool operator== (const query_entry &rhs) const {
                return thread_id == rhs.thread_id &&
                    query_time == rhs.query_time &&
                    error_code == rhs.error_code &&
                    database == rhs.database &&
                    statement == rhs.statement;
            }
            uint32_t thread_id;
            uint32_t query_time;
            uint16_t error_code;
            std::string database;
            std::string statement;
        };

        /** */
        struct rand_entry {
            rand_entry () : seed_1 (0), seed_2 (0) { }
            rand_entry (const struct event_buffer &evbuf);
            bool operator== (const rand_entry &rhs) const { 
                return seed_1 == rhs.seed_1 && seed_2 == rhs.seed_2;
            }
            uint64_t seed_1;
            uint64_t seed_2;
        };

        /** */
        struct intvar_entry {
            intvar_entry () : type (0), value (0) { }
            intvar_entry (const struct event_buffer &evbuf);
            bool operator== (const intvar_entry &rhs) const { 
                return type == rhs.type && value == rhs.value; 
            }
            uint8_t type;
            uint64_t value;
        };

        /** */
        struct rotate_entry {
            rotate_entry () : next_position (0) { }
            rotate_entry (const struct event_buffer &evbuf);
            bool operator== (const rotate_entry &rhs) const { 
                return next_position == rhs.next_position && next_file == rhs.next_file; 
            }
            uint64_t next_position;
            std::string next_file;
        };

        /** */
        struct xid_entry {
            xid_entry () : id (0) { }
            xid_entry (const struct event_buffer &evbuf);
            bool operator== (const xid_entry &rhs) const { 
                return id == rhs.id;
            }
            uint64_t id;
        };

        /** */
        struct entry {
            entry ();
            entry (const entry &rhs);
            entry (const event_buffer *evbuf);
            entry& operator= (const entry &rhs);
            bool operator== (const entry &rhs) const;
            ~entry ();

            event_buffer* get_buffer () { return &m_buffer; }
            const event_buffer* get_buffer () const { return &m_buffer; }
            
            /*
              If this was nice and actually to be used from c++, it
              would have something ala a visitor patten to convert and
              pass the right subtype to a visitor.
             */
        private:
            event_buffer m_buffer;
        };

        /** */
        struct iterator {
            typedef ptrdiff_t difference_type;
            typedef std::forward_iterator_tag iterator_category;
            typedef entry value_type;
            // The astute reader will notice the this iterator's
            // traits shows that reference is a value_type. This is
            // because the iterator is generating values. The entries
            // don't exist in the parent binlog, but are created on
            // operator++ calls. Therefore, to ensure that the value
            // survives the iterator for the python bindings...  it's
            // a value rather than reference. Yes, it's copy heavy.
            // Also, this isn't a problem in C++ since reference &
            // scope enforcement ensures things will work. 
            // This also makes operator-> sad.
            typedef value_type reference;
            typedef const value_type const_reference;
            typedef value_type* pointer;
            typedef const value_type* const_pointer;
            reference operator* () { return m_entry; }
            const_reference operator* () const { return m_entry; }
            pointer operator-> () { return &m_entry; }
            const_pointer operator-> () const { return &m_entry; }

            /* 
            // "Proper" C++ definition, but see above for why not. 
            // Making python bindings less angry.
            typedef value_type& reference;
            typedef const value_type& const_reference;
            typedef value_type* pointer;
            typedef const value_type* const_pointer;
            reference operator* () { return m_entry; }
            const_reference operator* () const { return m_entry; }
            pointer operator-> () { return &(operator* ()); }
            const_pointer operator-> () const { return &(operator* ()); }
            */

            bool operator== (const iterator &rhs) const {
                return m_entry == rhs.m_entry;
            }
            bool operator!= (const iterator &rhs) const { return ! operator== (rhs); }

            iterator& operator++ ();
            iterator operator++ (int) {
                iterator tmp (*this);
                ++(*this);
                return tmp;
            }

            iterator () : m_entry (), m_binlog (NULL) { }
            iterator (const iterator &rhs);

        private:
            iterator (const entry &ev, binlog *binlog) : m_entry (ev), m_binlog (binlog) { }

            // Verboten for now, feel free to add if you need...
            iterator& operator= (const iterator& rhs);

            value_type m_entry;
            binlog *m_binlog;

            friend class binlog;
        };

        /** Constructor
            \param filename name of the binlog file to process
            \param starting_offset start reading from first entry after this offset
            \param starting_time start reading from first entry closest to this time
            \note time trumphs offset
        */
        binlog (const std::string &filename, off64_t starting_offset, time_t starting_time);

        /** Constructor
            \param fd file to read, must be positioned right
        */
        binlog (int fd);

        virtual ~binlog ();

        iterator begin () { return iterator (m_evbuf, this); }
        iterator end () { return iterator (); }

    private:
        /**
         * Check if a file looks valid (binlog version, magic bytes etc
         */
        int check_file (struct event_buffer *evbuf);

        /**
         * Check to see if an event looks valid.
         **/
        int check_event (struct event_buffer *evbuf);

        /**
         * Read an event from the specified offset into the given event buffer.
         *
         * Will also malloc() space for any dynamic portions of the event, if the
         * event passes check_event.
         **/
        int read_event (struct event_buffer *evbuf, off64_t offset);

        /**
         * Find the offset of the next event after the one passed in.
         * Uses the built-in event chaining.
         *
         * Usage:
         *  struct event_buffer *next;
         *  struct event_buffer *evbuf = ...
         *  off_t next_offset = next_after(evbuf);
         *  read_event(fd, next, next_offset);
         **/
        off64_t next_after (struct event_buffer *evbuf);

        /*
         * Get the first event after starting_offset in fd
         *
         * If evbuf is non-null, copy it into there
         */
        off64_t nearest_offset (off64_t starting_offset, struct event_buffer *outbuf, int direction);

        /**
         * Binary-search to find the record closest to the requested time
         **/
        int nearest_time (time_t target, struct event_buffer *outbuf);

    private:
        int m_fd;
        bool m_owns_file;
        struct stat *m_stbuf;
        event_buffer *m_evbuf;
        time_t m_min_timestamp;
        time_t m_max_timestamp;
    };


    std::ostream& operator<< (std::ostream &os, const event_buffer &evbuf);

    std::ostream& operator<< (std::ostream &os, const binlog::entry &entry) {
        return os << *entry.get_buffer ();
    }
}


#endif /* YELP_YBINLOGP_H */
