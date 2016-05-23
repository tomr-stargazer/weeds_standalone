import numpy
import os
from math import log10
import cache

NotFoundError = cache.NotFoundError
blankPartfunc = cache.blankPartfunc
origins = cache.origins
timeout = 60
dbVersion = "1.00"

class Db:
   """
   Molecular and atomic line database

   This class defines a molecular and line database that can be
   searched online or offline. Database can be searched online
   through the SLAP protocol, or directly with HTTP POST or GET
   requests. Databases can also be fetched entirely and cached on a
   disk as an a SQLite database for offline searches.

   """

   def __init__(self, url, cache_file, protocol, online = True,
             name = ""):
      """
      Create a database instance

      This function create a database instance which is accessed
      through a given protocol. Supported protocols are "slap",
      "cdms_post" and "jpl_post".

      Arguments:
      url       -- The URL of the database
      protocol   -- The database protocol
      cache_file  -- The name of cache file
      online     -- Search the online database (default True)
      name      -- The name of the database (default "")
      dbout -- where the writes go

      """

      if not protocol in ["slap", "cdms_post", "jpl_post", "catfile", "local"]:
         raise ValueError, "Unknown protocol"
      if online:
         if protocol in ["catfile", "local"]:
            raise ValueError, "Local db or catfile cannot be online"
         #else:
            # Should try a request to see whether it is available
      else:
         if not protocol in ["catfile", "local"] and not isDbFile(cache_file):
            raise ValueError, "Offline cache not available"

      class linedb_data_class():
         """
         Storage class for linedb run time data

         """

      self.url = url
      self.protocol = protocol
      self.cache_file = os.path.expanduser(cache_file)
      self.online = online
      self.data = linedb_data_class()
      self.name = name
