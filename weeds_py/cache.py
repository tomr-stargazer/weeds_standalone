# cache.py -- Classes and methods to cache a line database.
# sqlite backend for the Local class in db.py

import sqlite3
import array
import os
from . import line
from datetime import datetime

origins = ["voparis", "vamdc", "splatalogue", "cdms", "jpl"]
blankPartfunc = -1

class NotFoundError(Exception):
   pass

class Cache:
   """
   Cache management class for linedb

   This class allows to create a local copy of a line database. This
   is done by fetching the entire (or part of) a database and storing
   it on the disk as a SQLite database.

   """

   def __init__(self, dbfile):
      self.db_file = dbfile

   def connect(self, new):
      """SQlite3-connect to the associated file and return the Connection
      instance.

      Parameters:
      -----------
       filename: the database file name
       new: if True, the file may or may not exist before.
            if False, the file must exist.
      """

      if not new:
        # File must exist. sqlite3.connect would create a new (empty)
        # file is not. This is not what we want.
        if not os.path.isfile(self.db_file):
          # File does not exist. This can happen if the database has
          # been deleted after the USE.
          raise Exception("Database file does not exist: "+self.db_file)

      return sqlite3.connect(self.db_file)


   def create(self, version, fmin=None, fmax=None, overwrite = False):
      """
      Create a SQL database containing the line catalog and
      partition functions

      Arguments:
      version   -- version of database
      fmin     -- minimal frequency
      fmax     -- maximal frequency
      overwrite -- overwrite existing db file (default False)

      """

      if os.path.isfile(self.db_file):
         if overwrite:
            os.remove(self.db_file)
         else:
            raise ValueError("Database file {0} already exists.".format(self.db_file))

      db_connect = self.connect(new=True)
      db_cursor = db_connect.cursor()
      db_cursor.execute("create table line ("
                    "'species' char(32),"
                    "'frequency' real,"
                    "'uncertainty' real,"
                    "'einstein_coefficient' real,"
                    "'upper_level_energy' real,"
                    "'upper_level_statistical_weight' real,"
                    "'upper_level_quantum_numbers' char(32),"
                    "'lower_level_energy' real,"
                    "'lower_level_statistical_weight' real,"
                    "'lower_level_quantum_numbers' char(32),"
                    "'origin' char(32),"
                    "'dbsource' char(32),"
                    "'date' char(32),"
                    "constraint sqod unique (species, upper_level_quantum_numbers, lower_level_quantum_numbers, origin, dbsource)"
                    ");")
# Those would most likely be the most useful indices. However, as sqlite
# implements a unique constraint as an index, we already have an, albeit
# not so good, index with specquantori. As a result, adding one of those
# indices would not improve things much, - actually some crude benchmarks
# showed that there's even a decrease in speed, probably because the engine
# has to take the time to evaluate which index is the most adequate -
# for quite the added cost in size. Hence why we decided against an index.
#      db_cursor.execute("create index 'lspecfreq' on line('species', 'frequency');")
#      db_cursor.execute("create index 'lorispecfreq' on line('origin', 'species', 'frequency');")
      db_cursor.execute("create table partfunc ("
                    "'species' char(32),"
                    "'temperature' blob,"
                    "'partfunc' blob,"
                    "'origin' char(32),"
                    "'dbsource' char(32),"
                    "constraint spec unique (species, origin, dbsource)"
                    ");")
      db_cursor.execute("create index 'pfspecies' on partfunc('species');")
      # Note: min_frequency, and max_frequency used to be in there, but we had
      # to get them from line table to keep them up to date anyway.
      # I'd remove the whole table altogether. Just keeping it around it case
      # the LAOG guys really want it.
      db_cursor.execute("create table info ("
                    "'version' char(32)"
                    ");")
      # Note: comma is necessary, otherwise it's not considered a tuple.
      db_cursor.execute("insert into info values (?);", (version,))
      db_cursor.execute("create trigger syncPartfunc after delete on line "
      "when (select species from line where species = old.species and origin = old.origin and dbsource = old.dbsource) is null "
      "begin delete from partfunc where species = old.species and origin = old.origin and dbsource = old.dbsource; end;"
      )

      db_connect.commit()
      db_cursor.close()

   @staticmethod
   def __execute_insert_line(db_cursor, line):
      # dirty hack: even though we're not in the upsert case, we still want to
      # update when the source is an online db or a .cat file, because we want
      # to keep track of the date.
      if line.dbsource in origins or ".cat" in line.dbsource:
         dt = datetime.utcnow().isoformat()
         action = 'insert or replace into line '
      else:
         dt = line.date
         action = 'insert into line '
      fr = "%.17f" % line.frequency
      err = "%.17f" % line.err_frequency
      ec = "%.17f" % line.einstein_coefficient
      ue = "%.17f" % line.upper_level.energy
      usw = "%.17f" % line.upper_level.statistical_weight
      le = "%.17f" % line.lower_level.energy
      lsw = "%.17f" % line.lower_level.statistical_weight
      db_cursor.execute(action + "values (?,?,?,?,?,?,?,?,?,?,?,?,?);",
                     (line.species, fr, err, ec, ue, usw, line.upper_level.quantum_numbers,
                      le, lsw, line.lower_level.quantum_numbers, line.origin, line.dbsource, dt)
                    )

   @staticmethod
   def __execute_upsert_line(db_cursor, line):
      if line.origin in origins:
         dt = datetime.utcnow().isoformat()
      else:
         dt = line.date
      fr = "%.17f" % line.frequency
      err = "%.17f" % line.err_frequency
      ec = "%.17f" % line.einstein_coefficient
      ue = "%.17f" % line.upper_level.energy
      usw = "%.17f" % line.upper_level.statistical_weight
      le = "%.17f" % line.lower_level.energy
      lsw = "%.17f" % line.lower_level.statistical_weight
      db_cursor.execute("insert or replace into line "
                       "values (?,?,?,?,?,?,?,?,?,?,?,?,?);",
                     (line.species, fr, err, ec, ue, usw, line.upper_level.quantum_numbers,
                      le, lsw, line.lower_level.quantum_numbers, line.origin, line.dbsource, dt)
                    )

   @staticmethod
   def __execute_insert_partfunc(db_cursor, species, temperature, partfunc, origin, dbsource):
      t = sqlite3.Binary(array.array('d', temperature).tostring())
      p = sqlite3.Binary(array.array('d', partfunc).tostring())

      query = '''insert into partfunc values(?,?,?,?,?)'''
      db_cursor.execute(query, (species, t, p, origin, dbsource))

   @staticmethod
   def __execute_upsert_partfunc(db_cursor, species, temperature, partfunc, origin, dbsource):

      t = sqlite3.Binary(array.array('d', temperature).tostring())
      p = sqlite3.Binary(array.array('d', partfunc).tostring())

      query = '''insert or replace into partfunc values(?,?,?,?,?)'''
      db_cursor.execute(query, (species, t, p, origin, dbsource))

   def add_line(self, line, update):
      """
      Add a line to the database

      Arguments:
      line -- line object

      """

      db_connect = self.connect(new=False)
      db_cursor = db_connect.cursor()
      try:
         if update:
            self.__execute_upsert_line(db_cursor, line)
         else:
            self.__execute_insert_line(db_cursor, line)
         db_connect.commit()
      except sqlite3.IntegrityError:
#         pysic.message(pysic.seve.w, "INSERT", "line ({0}, {1}, {2}, {3}) already present, not inserting.".format(line.species, line.dbSource, line.upper_level.energy, line.lower_level.energy))
         # the line already existed, that's fine.
         pass
      db_cursor.close()

   def add_lines(self, lines, update):
      """
      Add a list of lines to the database

      Arguments:
      lines -- line list

      """

      if update:
         f = self.__execute_upsert_line
      else:
         f = self.__execute_insert_line
      db_connect = self.connect(new=False)
      db_cursor = db_connect.cursor()
      for line in lines:
         try:
            f(db_cursor, line)
         except sqlite3.IntegrityError as error:
            #print "{}, {}, {}".format(line.species, line.upper_level.quantum_numbers, line.lower_level.quantum_numbers)
#            pysic.message(pysic.seve.w, "INSERT", "line ({0}, {1}, {2}, {3}) already present, not inserting.".format(line.species, line.dbSource, line.upper_level.energy, line.lower_level.energy))
            # the line already existed, that's fine.
            pass
      db_connect.commit()
      db_cursor.close()

   def add_partfunc(self, species, temperature, partfunc, origin, dbsource, update):
      """
      Add a partition function to the database

      Arguments:
      species     -- the species name
      temperature -- list of temperatures
      partfunc   -- list of partition function values at
                  these temperatures

      """

      db_connect = self.connect(new=False)
      db_cursor = db_connect.cursor()
      try:
         if update:
            self.__execute_upsert_partfunc(db_cursor, species, temperature,
                              partfunc, origin, dbsource)
         else:
            self.__execute_insert_partfunc(db_cursor, species, temperature,
                              partfunc, origin, dbsource)
         db_connect.commit()
      except sqlite3.IntegrityError:
         # the line already existed, that's fine.
         pass
      db_cursor.close()

#   def add_partfuncs(self, species, temperatures, partfuncs, origin, update):
   def add_partfuncs(self, stpod, update):
      """
      Add a partition function to the database

      Arguments:
      stpod -- list of (species, temperatures, partfuncs, origin, dbsource) tuples with:
      species     -- species names
      temperatures -- list of temperatures
      partfuncs   -- list of partition function values at
                  these temperatures
      """

      if update:
         f = self.__execute_upsert_partfunc
      else:
         f = self.__execute_insert_partfunc
      db_connect = self.connect(new=False)
      db_cursor = db_connect.cursor()
#      for spec, temperature, partfunc in zip(species, temperatures, partfuncs, origin):
      for spec, temperature, partfunc, origin, dbsource in stpod:
         try:
            f(db_cursor, spec, temperature, partfunc, origin, dbsource)
         except sqlite3.IntegrityError:
            # the line already existed, that's fine.
            pass
      db_connect.commit()
      db_cursor.close()

   # This does not work anymore since we added origin, because the "IN"
   # clause cannot work with tuples.
   # -> created a trigger instead. I'm keeping it around for a while,
   # just in case.

   # N.B: It is ok to do that instead of a trigger because we do not have
   # any concurrency. Otherwise, we would at least need to use that one
   # atomically with the delete operation on the line table.
#   def updatePartfuncOnLineDelete(self):
#      """
#      Enforce partfunc integrity in case a species is completely gone
#      from the line table after some delete(s).
#      """
#      db_connect = sqlite3.connect(self.db_file)
#      db_connect.execute("delete from partfunc where species in "
#         "(select species, origin from partfunc except "
#         "select species, origin from line group by species);")
#      db_connect.commit()

   def info(self):
      """
      Display informations on the database

      """

      db_connect = self.connect(new=False)
      db_connect.row_factory = sqlite3.Row
      db_cursor = db_connect.cursor()
      db_cursor.execute( "select * from info")
      row = db_cursor.fetchone()
      version = row['version']
      db_cursor.execute( "select MIN(frequency) as fmin, MAX(frequency) as fmax from line")
      row = db_cursor.fetchone()
      fmin = row['fmin']
      fmax = row['fmax']
      db_connect.commit()
      db_cursor.close()
      print()
      print('****** Database information ******')
      print('* version:           %12s *' % version)
      print('* minimal frequency: %12s *' % fmin)
      print('* maximal frequency: %12s *' % fmax)
      print('***********************************')
      print()

   def remove(self, lines):
      db_connect = self.connect(new=False)
      db_connect.row_factory = sqlite3.Row

      query = "delete from line"

      for l in lines:
         lquery = query + " where species = ? and origin = ? and dbsource = ?"
         lquery += " and upper_level_quantum_numbers = ?"
         lquery += " and lower_level_quantum_numbers = ?"
         args = (l.species, l.origin, l.prev, l.upper_level.quantum_numbers, l.lower_level.quantum_numbers)
         db_connect.execute(lquery, args)

      db_connect.commit()

   def search(self, fmin=-1, fmax=-1, species=[], origin='All', dbsource='All', energy=-1, einstein=-1):
      """
      Search lines in the cache

      Arguments:
      fmin   -- the minimum frequency in MHz
      fmax   -- the maximum frequency in MHz
      species -- the species name (a string or list of strings). String 'All'
                 is an alias for no selection.
      origin -- (default All)
      energy -- maximum upper level energy expressed
              in Kelvins (default -1)
      einstein -- coefficient to match (default -1)

      """

      if (type(species) == str):
        if (species == 'All'):
          lspecies = []  # Empty list (no selection by species)
        else:
          lspecies = [species]  # Store single string to a list
      elif (type(species) == list):
        lspecies = species
      else:
        raise Exception("Unexpected kind of argument: "+repr(species))

      db_connect = self.connect(new=False)
      db_connect.row_factory = sqlite3.Row
      db_cursor = db_connect.cursor()

      lines = []
      if fmin < 0:
         fmin = 0
      fmi = "%f" % fmin
      args = (fmi, )
      query = "select * from line where frequency >= ?"
      if fmax > 0:
         fma = "%f" % fmax
         query += " and frequency <= ?"
         args = args + (fma, )
      started = False
      for s in lspecies:
         # NB: AND logical operator has precedence over OR. Must use parenthesis e.g.
         # ... and ( species = AA or species like B* or species = CC )
         s = s.replace('*','%')
         query += (" and (" if not started else "or") + " species " + ('like' if ('%' in s) else '=') + " ? "
         args = args + (s, )
         started = True
      if started:
         query += ")"
      if origin != "All":
         query += " and origin = ?"
         args = args + (origin.lower(), )  # Case-insensitive search
      if dbsource != "All":
         query += " and dbsource = ?"
         args = args + (dbsource, )
      if energy > 0:
         uel = "%f" % energy
         query += " and upper_level_energy <= ?"
         args = args + (uel, )
      if einstein > 0:
         ein = "%f" % einstein
         query += " and einstein_coefficient >= ?"
         args = args + (ein, )

      db_cursor.execute(query, args)
      db_connect.commit()
      for row in db_cursor:
         l = line.line()
         l.species = row ['species']
         l.frequency = row ['frequency']
         l.err_frequency = row ['uncertainty']
         l.einstein_coefficient = row ['einstein_coefficient']
         l.upper_level.energy = row ['upper_level_energy']
         l.upper_level.statistical_weight = row ['upper_level_statistical_weight']
         l.upper_level.quantum_numbers = row ['upper_level_quantum_numbers']
         l.lower_level.energy = row ['lower_level_energy']
         l.lower_level.statistical_weight = row ['lower_level_statistical_weight']
         l.lower_level.quantum_numbers = row ['lower_level_quantum_numbers']
         l.origin = row ['origin']
         l.prev = row ['dbsource'] # we need to keep that info as well, for when calling part_func, while inserting lines.
         l.date = row ['date']
         lines.append(l)

      db_cursor.close()

      return lines

   def partition_function(self, species, origin, dbsource):
      """
      Returns the partition function at different temperatures

      This function get the partition function of the given species
      for different temperatures from the cache.

      Arguments:
      species -- the species name

      """

      db_connect = self.connect(new=False)
      db_connect.row_factory = sqlite3.Row
      db_cursor = db_connect.cursor()

      spec = "%s" % species
      ori = "%s" % origin
      dbsrc = "%s" % dbsource
      args = (spec, ori, dbsrc)
      query = "select * from partfunc where species = ? and origin = ? and dbsource = ?;"
      db_cursor.execute(query, args)

      temperature = array.array('d', [])
      partfunc = array.array('d', [])

      for row in db_cursor:
         temperature.fromstring(row['temperature'])
         partfunc.fromstring(row['partfunc'])
         break

      if len(partfunc) == 0 or partfunc[0] == blankPartfunc:
         raise NotFoundError("No partition function found for ({0}, {1}, {2}).".format(species, origin, dbsource))

      # TODO: all the other partition_function() impl return lists, so maybe
      # that one should do as well (instead of array.arrays). It seems to work fine
      # like this though.
      return temperature, partfunc

def isDbFile(dbfile):
   try:
      conn = sqlite3.connect(dbfile)
      c = conn.cursor()
      c.execute("select * from info;")
      c.close()
      return True
   except Exception:
      return False
