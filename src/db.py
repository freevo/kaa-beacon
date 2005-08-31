import string, os, time, re, math, cPickle
from sets import Set
from pysqlite2 import dbapi2 as sqlite
from kaa.base.utils import utf8

CREATE_SCHEMA = """
    CREATE TABLE meta (
        attr        TEXT UNIQUE, 
        value       TEXT
    );
    INSERT INTO meta VALUES('keywords_filecount', 0);
    INSERT INTO meta VALUES('version', 0.1);

    CREATE TABLE types (
        id              INTEGER PRIMARY KEY AUTOINCREMENT, 
        name            TEXT UNIQUE,
        attrs_pickle    BLOB
    );

    CREATE TABLE words (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        word            TEXT,
        count           INTEGER
    );
    CREATE UNIQUE INDEX words_idx on WORDS (word) ON CONFLICT REPLACE;

    CREATE TABLE words_map (
        rank            INTEGER,
        word_id         INTEGER,
        object_type     INTEGER,
        object_id       INTEGER,
        frequency       FLOAT
    );
    CREATE INDEX words_map_word_idx ON words_map (word_id, rank, object_type);
    CREATE INDEX words_map_object_idx ON words_map (object_type, object_id);
"""


ATTR_SIMPLE            = 0x00
ATTR_SEARCHABLE        = 0x01      # Is a SQL column, not a pickled field
ATTR_INDEXED           = 0x02      # Will have an SQL index
ATTR_KEYWORDS          = 0x04      # Also indexed for keyword queries

ATTR_KEYWORDS_FILENAME = 0x100     # Treat as filename for keywords index

STOP_WORDS = (
    "about", "and", "are", "but", "com", "for", "from", "how", "not", 
    "some", "that", "the", "this", "was", "what", "when", "where", "who", 
    "will", "with", "the", "www", "http", "org"
)

# Word length limits for keyword indexing
MIN_WORD_LENGTH = 2
MAX_WORD_LENGTH = 30

# These are special attributes for querying.  Attributes with
# these names cannot be registered.
RESERVED_ATTRIBUTES = ("parent", "object", "keywords", "type", "limit")

class Database:
    def __init__(self, dbfile = None):
        if not dbfile:
            dbfile = "kaavfs.sqlite"

        self._object_types = {}
        self._dbfile = dbfile
        self._open_db()

    def __del__(self):
        self.commit()

    def _open_db(self):
        self._db = sqlite.connect(self._dbfile)
        self._cursor = self._db.cursor()
        self._cursor.execute("PRAGMA synchronous=OFF")
        self._cursor.execute("PRAGMA count_changes=OFF")
        self._cursor.execute("PRAGMA cache_size=50000")

        if not self.check_table_exists("meta"):
            self._db.close()
            self._create_db()

        self._load_object_types()

    def _db_query(self, statement, args = ()):
        self._cursor.execute(statement, args)
        rows = self._cursor.fetchall()
        return rows

    def _db_query_row(self, statement, args = ()):
        rows = self._db_query(statement, args)
        if len(rows) == 0:
            return None
        return rows[0]

    def check_table_exists(self, table):
        res = self._db_query_row("SELECT name FROM sqlite_master where " \
                         "name=? and type='table'", (table,))
        return res != None


    def _create_db(self):
        try:
            os.unlink(self._dbfile)
        except:
            pass
        f = os.popen("sqlite3 %s" % self._dbfile, "w")
        f.write(CREATE_SCHEMA)
        f.close()
        self._open_db()

        self.register_object_type_attrs("dir")


    def register_object_type_attrs(self, type_name, attr_list = ()):
        if type_name in self._object_types:
            # This type already exists.  Compare given attributes with
            # existing attributes for this type.
            cur_type_id, cur_type_attrs = self._object_types[type_name]
            new_attrs = {}
            db_needs_update = False
            for name, type, flags in attr_list:
                if name not in cur_type_attrs:
                    new_attrs[name] = type, flags
                    if flags:
                        # New attribute isn't simple, needs to alter table.
                        db_needs_update = True

            if len(new_attrs) == 0:
                # All these attributes are already registered; nothing to do.
                return

            if not db_needs_update:
                # Only simple (i.e. pickled only) attributes are added, so we
                # don't need to alter the table, just update the types table.
                cur_type_attrs.update(new_attrs)
                self._db_query("UPDATE types SET attrs_pickle=? WHERE id=?",
                           (buffer(cPickle.dumps(cur_type_attrs, 2)), cur_type_id))
                return

            # Update the attr list to merge both existing and new attributes.
            # We need to update the database now.
            attr_list = []
            for name, (type, flags) in cur_type_attrs.items() + new_attrs.items():
                attr_list.append((name, type, flags))

        else:
            new_attrs = {}
            cur_type_id = None
            # Merge standard attributes with user attributes for this type.
            attr_list = (
                ("id", int, ATTR_SEARCHABLE),
                ("name", str, ATTR_KEYWORDS | ATTR_KEYWORDS_FILENAME),
                ("parent_id", int, ATTR_SEARCHABLE),
                ("parent_type", int, ATTR_SEARCHABLE),
                ("size", int, ATTR_SIMPLE),
                ("mtime", int, ATTR_SEARCHABLE),
                ("pickle", buffer, ATTR_SEARCHABLE),
            ) + tuple(attr_list)

        table_name = "objects_%s" % type_name

        create_stmt = "CREATE TABLE %s_tmp ("% table_name

        # Iterate through type attributes and append to SQL create statement.
        attrs = {}
        for name, type, flags in attr_list:
            assert(name not in RESERVED_ATTRIBUTES)
            # If flags is non-zero it means this attribute needs to be a
            # column in the table, not a pickled value.
            if flags:
                sql_types = {str: "TEXT", int: "INTEGER", float: "FLOAT", 
                             buffer: "BLOB", unicode: "TEXT"}
                assert(type in sql_types)
                create_stmt += "%s %s" % (name, sql_types[type])
                if name == "id":
                    # Special case, these are auto-incrementing primary keys
                    create_stmt += " PRIMARY KEY AUTOINCREMENT"
                create_stmt += ","

            attrs[name] = (type, flags)

        create_stmt = create_stmt.rstrip(",") + ")"
        self._db_query(create_stmt)

        # Add this type to the types table, including the attributes
        # dictionary.
        self._db_query("INSERT OR REPLACE INTO types VALUES(?, ?, ?)", 
                       (cur_type_id, type_name, buffer(cPickle.dumps(attrs, 2))))

        if new_attrs:
            # Migrate rows from old table to new one.
            columns = filter(lambda x: cur_type_attrs[x][1], cur_type_attrs.keys())
            columns = string.join(columns, ",")
            self._db_query("INSERT INTO %s_tmp (%s) SELECT %s FROM %s" % \
                           (table_name, columns, columns, table_name))

            # Delete old table.
            self._db_query("DROP TABLE %s" % table_name)

        # Rename temporary table.
        self._db_query("ALTER TABLE %s_tmp RENAME TO %s" % \
                       (table_name, table_name))


        # Create index for locating object by full path (i.e. parent + name)
        self._db_query("CREATE UNIQUE INDEX %s_parent_name_idx on %s "\
                       "(parent_id, parent_type, name)" % \
                       (table_name, table_name))
        # Create index for locating all objects under a given parent.
        self._db_query("CREATE INDEX %s_parent_idx on %s (parent_id, "\
                       "parent_type)" % (table_name, table_name))

        # If any of these attributes need to be indexed, create the index
        # for that column.  TODO: need to support indexes on multiple
        # columns.
        for name, type, flags in attr_list:
            if flags & ATTR_INDEXED:
                self._db_query("CREATE INDEX %s_%s_idx ON %s (%s)" % \
                               (table_name, name, table_name, name))

        self._load_object_types()


    def _load_object_types(self):
        for id, name, attrs in self._db_query("SELECT * from types"):
            self._object_types[name] = id, cPickle.loads(str(attrs))
    

    def _make_query_from_attrs(self, query_type, attrs, type_name):
        type_attrs = self._object_types[type_name][1]

        columns = []
        values = []
        placeholders = []

        for key in attrs.keys():
            if attrs[key] == None:
                del attrs[key]
        attrs_copy = attrs.copy()
        for name, (type, flags) in type_attrs.items():
            if flags != ATTR_SIMPLE and name in attrs:
                columns.append(name)
                placeholders.append("?")
                if name in attrs:
                    if type == str:
                        values.append(utf8(attrs[name]))
                    else:
                        values.append(attrs[name])
                    del attrs_copy[name]
                else:
                    values.append(None)

        if len(attrs_copy) > 0:
            columns.append("pickle")
            values.append(buffer(cPickle.dumps(attrs_copy, 2)))
            placeholders.append("?")

        table_name = "objects_" + type_name

        if query_type == "add":
            columns = string.join(columns, ",")
            placeholders = string.join(placeholders, ",")
            q = "INSERT INTO %s (%s) VALUES(%s)" % (table_name, columns, placeholders)
        else:
            q = "UPDATE %s SET " % table_name
            for col, ph in zip(columns, placeholders):
                q += "%s=%s," % (col, ph)
            # Trim off last comma
            q = q.rstrip(",")
            q += " WHERE id=?"
            values.append(attrs["id"])

        return q, values
    

    def delete_object(self, (object_type, object_id)):
        """
        Deletes the specified object.
        """
        # TODO: recursively delete all children of this object.
        self._delete_object_keywords((object_type, object_id))
        self._db_query("DELETE FROM objects_%s WHERE id=?" % \
                       object_type, (object_id,))
        

    def add_object(self, (object_type, object_name), parent = None, **attrs):
        """
        Adds an object to the database.   When adding, an object is identified
        by a (type, name) tuple.  Parent is a (type, id) tuple which refers to
        the object's parent.  In both cases, "type" is a type name as 
        given to register_object_type_attrs().  attrs kwargs will vary based on
        object type.  ATTR_SIMPLE attributes which a None are not added.

        This method returns the dict that would be returned if this object
        were queried by query_normalized().
        """
        type_attrs = self._object_types[object_type][1]
        if parent:
            attrs["parent_type"] = self._object_types[parent[0]][0]
            attrs["parent_id"] = parent[1]
        attrs["name"] = object_name
        query, values = self._make_query_from_attrs("add", attrs, object_type)
        self._db_query(query, values)

        # Add id given by db, as well as object type.
        attrs["id"] = self._cursor.lastrowid
        attrs["type"] = object_type

        # Index keyword attributes
        word_parts = []
        for name, (type, flags) in type_attrs.items():
            if name in attrs and flags & ATTR_KEYWORDS:
                word_parts.append((attrs[name], 1.0, flags))
        words = self._score_words(word_parts)
        self._add_object_keywords((object_type, attrs["id"]), words)

        # For attributes which aren't specified in kwargs, add them to the
        # dict we're about to return, setting default value to None.
        for name, (type, flags) in type_attrs.items():
            if name not in attrs:
                attrs[name] = None

        return attrs


    def update_object(self, (object_type, object_id), parent = None, **attrs):
        """
        Update an object in the database.  For updating, object is identified
        by a (type, id) tuple.  Parent is a (type, id) tuple which refers to
        the object's parent.  If specified, the object is reparented,
        otherwise the parent remains the same as when it was added with
        add_object().  attrs kwargs will vary based on object type.  If a
        ATTR_SIMPLE attribute is set to None, it will be removed from the
        pickled dictionary.
        """
        type_attrs = self._object_types[object_type][1]
        needs_keyword_reindex = False
        keyword_columns = []
        for name, (type, flags) in type_attrs.items():
            if flags & ATTR_KEYWORDS:
                if name in attrs:
                    needs_keyword_reindex = True
                keyword_columns.append(name)

        q = "SELECT pickle%%s FROM objects_%s WHERE id=?" % object_type
        if needs_keyword_reindex:
            q %= "," + ",".join(keyword_columns)
        else:
            q %= ""
        
        row = self._db_query_row(q, (object_id,))
        assert(row)
        if row[0]:
            row_attrs = cPickle.loads(str(row[0]))
            row_attrs.update(attrs)
            attrs = row_attrs
        if parent:
            attrs["parent_type"] = self._object_types[parent[0]][0]
            attrs["parent_id"] = parent[1]
        attrs["id"] = object_id
        query, values = self._make_query_from_attrs("update", attrs, object_type)
        self._db_query(query, values)

        if needs_keyword_reindex:
            # We've modified a ATTR_KEYWORD column, so we need to reindex all
            # all keyword attributes for this row.

            # Merge the other keyword columns into attrs dict.
            for n, name in zip(range(len(keyword_columns)), keyword_columns):
                if name not in attrs:
                    attrs[name] = row[n + 1]

            # Remove existing indexed words for this object.
            self._delete_object_keywords((object_type, object_id))

            # Re-index 
            word_parts = []
            for name, (type, flags) in type_attrs.items():
                if flags & ATTR_KEYWORDS:
                    word_parts.append((attrs[name], 1.0, flags))
            words = self._score_words(word_parts)
            self._add_object_keywords((object_type, object_id), words)


    def commit(self):
        self._db.commit()

    def query(self, **attrs):
        """
        Query the database for objects matching all of the given attributes
        (specified in kwargs).  There are a few special kwarg attributes:

             parent: (type, id) tuple referring to the object's parent, where
                     type is the name of the type.
             object: (type, id) tuple referring to the object itself.
           keywords: a string of search terms for keyword search.
               type: only search items of this type (e.g. "images"); if None
                     (or not specified) all types are searched.
              limit: return only this number of results; if None (or not 
                     specified) all matches are returned.  For better
                     performance it is highly recommended a limit is specified
                     for keyword searches.

        Return value is a raw ("unnormalized") list of results that match the
        query where each item is in the form (columns, type_name, rows), where
        rows is a list of tuples for each row, type_name is the naem of the
        type these rows belong to, and columns is a list of column names,
        where each item in columns corresponds to an item in each row.

        This raw list can be passed to normalize_query_results() which will
        return a list of dicts for more convenient use.
        """
        # FIXME: Keyword searches lose sort order (by rank).  Fixing this
        # will require changing the return value.
        if "object" in attrs:
            attrs["type"], attrs["id"] = attrs["object"]
            del attrs["object"]

        if "keywords" in attrs:
            # If search criteria other than keywords are specified, we can't
            # enforce a limit on the keyword search, otherwise we might miss
            # intersections.
            # TODO: Possible optimization: do keyword search after the query
            # below only on types that have results iff all queried columns are
            # indexed.
            if len(Set(attrs).difference(("type", "limit", "keywords"))) > 0:
                limit = None 
            else: 
                limit = attrs.get("limit") 
            results = self._query_keywords(attrs["keywords"], limit, 
                                           attrs.get("type"))

            # No matches to our keyword search, so we're done.
            if not results:
                return []

            computed_object_ids = []
            for tp, id in results:
                computed_object_ids.append(tp*10000000 + id)
            del attrs["keywords"]
        else:
            computed_object_ids = None


        if "type" in attrs:
            type_list = [(attrs["type"], self._object_types[attrs["type"]])]
            del attrs["type"]
        else:
            type_list = self._object_types.items()

        if "parent" in attrs:
            parent_type, parent_id = attrs["parent"]
            attrs["parent_type"] = self._object_types[parent_type][0]
            attrs["parent_id"] = parent_id
            del attrs["parent"]

        if "limit" in attrs:
            result_limit = attrs["limit"]
            del attrs["limit"]
        else:
            result_limit = None

        results = []
        for type_name, (type_id, type_attrs) in type_list:
            # List of attribute dicts for this type.
            columns = filter(lambda x: type_attrs[x][1], type_attrs.keys())

            # Construct a query based on the supplied attributes for this
            # object type.  If any of the attribute names aren't valid for
            # this type, then we don't bother matching, since this an AND
            # query and there aren't be any matches.
            if len(Set(attrs).difference(columns)) > 0:
                continue

            q = "SELECT '%s',%s%%s FROM objects_%s" % \
                (type_name, string.join(columns, ","), type_name)

            if computed_object_ids != None:
                q %= ",%d+id as computed_id" % (type_id * 10000000)
                q +=" WHERE computed_id IN %s" % self._list_to_utf8_printable(computed_object_ids)
            else:
                q %= ""

            query_values = []
            for attr, value in attrs.items():
                if q.find("WHERE") == -1:
                    q += " WHERE "
                else:
                    q += " AND "

                q += "%s=?" % attr
                query_values.append(value)
            
            if result_limit != None:
                q += " LIMIT %d" % result_limit

            rows = self._db_query(q, query_values)
            #results.append((columns_dict, type_name, row))
            #results.extend(rows)
            results.append((["type"] + columns, type_name, rows))

        return results

    def query_normalized(self, **attrs):
        """
        Performs a query as in query() and returns normalized results.
        """
        return self.normalize_query_results(self.query(**attrs))

    def normalize_query_results(self, results):
        """
        Takes a results list as returned from query() and converts to a list
        of dicts.  Each result dict is given a "type" entry which corresponds 
        to the type name of that object.
        """
        new_results = []
        for columns, type_name, rows in results:
            for row in rows:
                result = dict(zip(columns, row))
                result["type"] = type_name
                if result["pickle"]:
                    pickle = cPickle.loads(str(result["pickle"]))
                    del result["pickle"]
                    result.update(pickle)
                new_results.append(result)
        return new_results


    def list_query_results_names(self, results):
        """
        Do a quick-and-dirty list of filenames given a query results list,
        sorted by filename.
        """
        # XXX: should be part of VFS, not database.
        files = []
        for columns, type_name, rows in results:
            filecol = columns.index("name")
            for row in rows:
                files.append(row[filecol])
        files.sort()
        return files

    def _score_words(self, text_parts):
        """
        Scores the words given in text_parts, which is a list of tuples
        (text, coeff, type), where text is the string of words
        to be scored, coeff is the weight to give each word in this part
        (1.0 is normal), and type is one of ATTR_KEYWORDS_*.

        Each word W is given the score:
             sqrt( (W coeff * W count) / total word count )

        Counts are relative to the given object, not all objects in the
        database.
        
        Returns a dict of words whose values hold the score caclulated as
        above.
        """
        words = {}
        total_words = 0

        for text, coeff, attr_type in text_parts:
            if not text:
                continue
            text = utf8(text)

            if attr_type & ATTR_KEYWORDS_FILENAME:
                dirname, filename = os.path.split(text)
                fname_noext, ext = os.path.splitext(filename)
                # Remove the first 2 levels (like /home/user/) and then take
                # the last two levels that are left.
                levels = dirname.strip('/').split(os.path.sep)[2:][-2:] + [fname_noext]
                parsed = re.split("[\s_\-()/\\\\[\]\"]", string.join(levels)) + [fname_noext]
            else:
                parsed = re.split('[\s_\-()/\\\\[\]\"]', text)

            for word in parsed:
                if not word or len(word) > MAX_WORD_LENGTH:
                    # Probably not a word.
                    continue
                word = word.lower()
                try:
                    word = word.decode("utf-8")
                except:
                    # FIXME: if this fails too, word isn't unicode.
                    pass

                if len(word) < MIN_WORD_LENGTH or word in STOP_WORDS:
                    continue
                if word not in words:
                    words[word] = coeff
                else:
                    words[word] += coeff
                total_words += 1

        # Score based on word frequency in document.  (Add weight for 
        # non-dictionary words?  Or longer words?)
        for word, score in words.items():
            words[word] = math.sqrt(words[word] / total_words)
        return words

    def _delete_object_keywords(self, (object_type, object_id)):
        """
        Removes all indexed keywords for the given object.  This function
        must be called when an object is removed from the database, or when
        an object is being updated (and therefore its keywords must be
        re-indexed).
        """
        # Resolve object type name to id
        object_type = self._object_types[object_type][0]

        self._db_query("UPDATE words SET count=count-1 WHERE id IN " \
                       "(SELECT word_id FROM words_map WHERE object_type=? AND object_id=?)",
                       (object_type, object_id))
        self._db_query("DELETE FROM words_map WHERE object_type=? AND object_id=?",
                       (object_type, object_id))

        # FIXME: We need to do this eventually, but there's no index on count,
        # so this could potentially be slow.  It doesn't hurt to leave rows
        # with count=0, so this could be done intermittently.
        #self._db_query("DELETE FROM words WHERE count=0")

        if self._cursor.rowcount > 0:
            self._db_query("UPDATE meta SET value=value-1 WHERE attr='keywords_filecount'")

    def _list_to_utf8_printable(self, items):
        """
        Takes a list of mixed types and outputs a utf-8 encoded string.  For
        example, a list [42, 'foo', None, "foo's string"], this returns the
        string:

            (42, 'foo', NULL, 'foo''s string')

        Single quotes are escaped as ''.  This is suitable for use in SQL 
        queries.
        """
        fixed_items = []
        for item in items:
            if type(item) in (int, long):
                fixed_items.append(str(item))
            elif item == None:
                fixed_items.append("NULL")
            elif type(item) in (str, unicode):
                fixed_items.append("'%s'" % utf8(item.replace("'", "''")))
            else:
                raise Exception, "Unsupported type '%s' given to list_to_utf8_printable" % type(item)

        return "(" + ",".join(fixed_items) + ")"

    def _add_object_keywords(self, (object_type, object_id), words):
        """
        Adds the dictionary of words (as computed by _score_words()) to the
        database for the given object.
        """
        # Resolve object type name to id
        object_type = self._object_types[object_type][0]

        # Holds any of the given words that already exist in the database
        # with their id and count.
        db_words_count = {}

        words_list = self._list_to_utf8_printable(words.keys())
        q = "SELECT id,word,count FROM words WHERE word IN %s" % words_list
        rows = self._db_query(q)
        for row in rows:
            db_words_count[row[1]] = row[0], row[2]

        # For executemany queries later.
        update_list, map_list = [], []

        for word, score in words.items():
            if word not in db_words_count:
                # New word, so insert it now.
                self._db_query("INSERT INTO words VALUES(NULL, ?, 1)", (word,))
                db_id, db_count = self._cursor.lastrowid, 1
                db_words_count[word] = db_id, db_count
            else:
                db_id, db_count = db_words_count[word]
                update_list.append((db_count + 1, db_id))

            map_list.append((int(score*10), db_id, object_type, object_id, score))

        self._cursor.executemany("UPDATE words SET count=? WHERE id=?", update_list)
        self._cursor.executemany("INSERT INTO words_map VALUES(?, ?, ?, ?, ?)", map_list)
        self._db_query("UPDATE meta SET value=value+1 WHERE attr='keywords_filecount'")


    def _query_keywords(self, words, limit = 100, object_type = None):
        """
        Queries the database for the keywords supplied in the words strings.
        (Search terms are delimited by spaces.)  

        The search algorithm tries to optimize for the common case.  When
        words are scored (_score_words()), each word is assigned a score that
        is stored in the database (as a float) and also as an integer in the
        range 0-10, called rank.  (So a word with score 0.35 has a rank 3.)

        Multiple passes are made over the words_map table, first starting at
        the highest rank fetching a certain number of rows, and progressively 
        drilling down to lower ranks, trying to find enough results to fill our
        limit that intersects on all supplied words.  If our limit isn't met
        and all ranks have been searched but there are still more possible 
        matches (because we use LIMIT on the SQL statement), we expand the
        LIMIT (currently by an order of 10) and try again, specifying an 
        OFFSET in the query.

        The worst case scenario is that all search terms exist in the database
        but there exist no intersection of the search terms where one of the
        terms has a very large number of hits.  This means we must search
        the whole words table only to find there are no matches.  This could
        be improved by avoiding the OFFSET/LIMIT technique as described above,
        but that approach provides a big performance win when there are
        matches.

        object_type specifies an type name to search (for example we can
        search type "image" with keywords "2005 vacation"), or if object_type
        is None (default), then all types are searched.

        This function returns a list of (object_type, object_id) tuples 
        which match the query.  The list is sorted by score (with the 
        highest score first).
        """

        # Fetch number of files that are keyword indexed.  (Used in score
        # calculations.)
        row = self._db_query_row("SELECT value FROM meta WHERE attr='keywords_filecount'")
        filecount = int(row[0])

        # Convert words string to a tuple of lower case words.
        words = tuple(words.lower().split())
        # Remove words that aren't indexed (words less than MIN_WORD_LENGTH 
        # characters, or and words in the stop list).
        words = filter(lambda x: len(x) >= MIN_WORD_LENGTH and x not in STOP_WORDS, words)
        words_list = self._list_to_utf8_printable(words)
        nwords = len(words)

        if nwords == 0:
            return []

        # Find word ids and order by least popular to most popular.
        rows = self._db_query("SELECT id,word,count FROM words WHERE word IN %s ORDER BY count" % words_list)
        words = {}
        ids = []
        for row in rows:
            words[row[0]] = {
                "word": row[1],
                "count": row[2],
                "idf_t": math.log(filecount / row[2] + 1) + 1
            }
            ids.append(row[0])
            print "WORD: %s (%d), freq=%d/%d, idf_t=%f" % (row[1], row[0], row[2], filecount, words[row[0]]["idf_t"])

        # Not all the words we requested are in the database, so we return
        # 0 results.
        if len(ids) < nwords:
            return []

        if object_type:
            # Resolve object type name to id
            object_type = self._object_types[object_type][0]

        results, state = {}, {}
        for id in ids:
            results[id] = {}
            state[id] = {
                "offset": [0]*11,
                "more": [True]*11
            }

        all_results = {}
        if limit == None:
            limit = filecount

        sql_limit = max(limit*3, 100)
        finished = False
        nqueries = 0

        while not finished:
            for rank in range(10, -1, -1):
                for id in ids:
                    if not state[id]["more"][rank]:
                        continue
                    t0=time.time()

                    q = "SELECT object_type,object_id,frequency FROM " \
                        "words_map WHERE word_id=? AND rank=? %s " \
                        "LIMIT ? OFFSET ?"
                    if object_type == None:
                        q %= ""
                        v = (id, rank, sql_limit, state[id]["offset"][rank])
                    else:
                        q %= "AND object_type=?"
                        v = (id, rank, object_type, sql_limit, state[id]["offset"][rank])

                    rows = self._db_query(q, v)
                    #print q, v, time.time()-t0
                    nqueries += 1
                    state[id]["more"][rank] = len(rows) == sql_limit

                    for row in rows:
                        results[id][row[0], row[1]] = row[2] * words[id]["idf_t"]

                # end loop over words
                for r in reduce(lambda a, b: Set(a).intersection(Set(b)), results.values()):
                    all_results[r] = 0
                    for id in ids:
                        if r in results[id]:
                            all_results[r] += results[id][r]

                # If we have enough results already, no sense in querying the
                # next rank.
                if limit > 0 and len(all_results) > limit*2:
                    finished = True
                    #print "Breaking at rank:", rank
                    break

            # end loop over ranks
            if finished:
                break

            finished = True
            for index in range(len(ids)):
                id = ids[index]

                if index > 0:
                    last_id = ids[index-1]
                    a = results[last_id]
                    b = results[id]
                    intersect = Set(a).intersection(b)

                    if len(intersect) == 0:
                        # Is there any more at any rank?
                        a_more = b_more = False
                        for rank in range(11):
                            a_more = a_more or state[last_id]["more"][rank]
                            b_more = b_more or state[id]["more"][rank]

                        if not a_more and not b_more:
                            # There's no intersection between these two search
                            # terms and neither have more at any rank, so we 
                            # can stop the whole query.
                            finished = True
                            break

                # There's still hope of a match.  Go through this term and
                # see if more exists at any rank, increasing offset and
                # unsetting finished flag so we iterate again.
                for rank in range(10, -1, -1):
                    if state[id]["more"][rank]:
                        state[id]["offset"][rank] += sql_limit
                        finished = False

            # If we haven't found enough results after this pass, grow our
            # limit so that we expand our search scope.  (XXX: this value may
            # need empirical tweaking.)
            sql_limit *= 10

        # end loop while not finished
        keys = all_results.keys()
        keys.sort(lambda a, b: cmp(all_results[b], all_results[a]))
        if limit > 0:
            keys = keys[:limit]

        #print "* Did %d subqueries" % (nqueries)
        return keys
        #return [ (all_results[file], file) for file in keys ]
        
