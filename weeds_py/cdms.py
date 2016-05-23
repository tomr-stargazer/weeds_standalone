# cdms.py -- linedb implementation for cdms

import os
import urllib
import urllib2
import urlparse
import math
import db
import line
from consts import *
from datetime import datetime

partfunc_url = "http://www.astro.uni-koeln.de/site/vorhersagen/catalog/partition_function.html"
maxFreqGHz = 2000

class Cdms(db.Db):
   def __post(self, fmin, fmax, species, energy, einstein):
      """
      Search lines in a the CDMS database using HTTP/POST method

      Arguments:
      fmin   -- the minimum frequency in MHz
      fmax   -- the maximum frequency in MHz
      species -- the species name (default All)
      Eu_max -- maximum upper level energy expressed
              in cm-1 (default None)

      """

      lines = []

      # Make a HTTP/POST query on the server. Results of the query are
      # stored in a cache file, so the request is a two step process:
      # first we get the URL of the cache file, and then we read this
      # file.

      if fmin > 0:
         fmin = "%.9f" % (fmin * 1e-3) # MHz -> GHz
      else:
         fmin = 0
      if fmax > 0:
         fmax = "%.9f" % (fmax * 1e-3) # MHz -> GHz
      else:
         fmax = maxFreqGHz

      # FixMe: It looks that there is a bug in the database. If one
      # select a large (a few GHz) frequency range and request the
      # einstein coefficients (temp=0), then one get an internal server
      # error (HTTP 500). Let's request for the S*\mu^2 values instead
      # (temp=1) until they fix that.

      # TODO(mpl): why not filter by species as soon as here? ask the laog guys
      form_values = {'MinNu': fmin, 'MaxNu': fmax , 'UnitNu': "GHz",
                  'Molecules': species, 'StrLim': "-10",
                  "temp": "0", "output": "text", "sort": "frequency",
                  "mol_sort_query": "tag", "logscale": "yes",
                  "autoscale": "yes"}

      try:

         data = urllib.urlencode(form_values)
         bdata = data.encode('utf-8')
         req = urllib2.Request(self.url, bdata)
         response = urllib2.urlopen(req, timeout = db.timeout)

         resp = response.read().decode('utf-8')
         base_url = urlparse.urlsplit(self.url).scheme + "://" \
            + urlparse.urlsplit(self.url).netloc
         cache_url = base_url \
            + resp.split("\n")[4].split('"')[1]

         req = urllib2.Request(cache_url)
         response = urllib2.urlopen(req, timeout = db.timeout)

         #print 'response=',response

      except Exception, error:
         raise Exception, "Could not connect to database: %s" % error

      # Parse the results.
      for l in response.readlines()[10:-1]:
         try:
            # Note: if this decoding ever becomes a bottleneck (which I doubt),
            # one could keep the lines as binary and do the decoding to
            # utf-8 string only where/when necessary.
            l = l.decode('utf-8')
            # NB: freq and errfreq are in MHz if errfreq>0, else in cm-1
            freq = float(l[0:13]) # MHz
            wavelength = speed_of_light / (freq * 1e6) * 1e2 # cm
            errfreq = float(l[13:24])  # MHz
            einstein_coefficient = 10**float(l[24:35])
            lower_level_energy = float(l[37:47]) * cm_K # K
            upper_level_statistical_weight = int(l[47:50])
            upper_level_quantum_numbers = l[61:73].split(None, 0)[0].rsplit(None, 0)[0]
            lower_level_quantum_numbers = l[73:88].split(None, 0)[0].rsplit(None, 0)[0]
            spec = l[88:].split(None, 0)[0].rsplit(None, 0)[0]

            #print 'l=',l

            # Drop the asterisk at the beginning of some species names
            if spec[0] == "*":
               spec = spec[1:]


            # filter by einstein coefficient, if required
            if einstein > 0 and einstein_coefficient < einstein:
                  continue

            sl =  line.line()
            sl.species = spec
            sl.frequency = freq
            sl.err_frequency = errfreq
            sl.einstein_coefficient = einstein_coefficient
            sl.upper_level.energy = lower_level_energy + cm_K / wavelength # K

            # filter by energy, if required
            if energy > 0 and sl.upper_level.energy > energy:
                  continue

            sl.upper_level.statistical_weight = upper_level_statistical_weight
            sl.upper_level.quantum_numbers = upper_level_quantum_numbers
            sl.lower_level.energy = lower_level_energy
            sl.lower_level.statistical_weight = upper_level_statistical_weight
            sl.lower_level.quantum_numbers = lower_level_quantum_numbers
            sl.origin = self.name
            sl.dbsource = self.name
            sl.date = datetime.utcnow().isoformat()

            lines.append(sl)

         except Exception, error:

            # FixMe: Some species have missing entries. Ignore them
            # for the moment.
            continue
#            raise Exception, "Can't parse the response from the database: %s" % error

      return lines

   def search(self, fmin, fmax, species='All', origin='All', dbsource='All', energy=-1, einstein=-1):
      """
      Search lines in a remote cdms db

      Arguments:
      fmin   -- the minimum frequency in MHz
      fmax   -- the maximum frequency in MHz
      species -- the species name (single string). String 'All' is an alias for
                 no selection. Tolerate a list as input with 0 (no selection
                 i.e. same as 'All') or 1 string element.
      Eu_max -- maximum upper level energy expressed
              in cm-1 (default None)
      """

      if origin != 'All' and origin.lower() != self.name:  # Case-insensitive
         return

      if dbsource != 'All' and dbsource != self.name:
         return

      if (type(species) == str):
        lspecies = species
      elif (type(species) == list):
        if (len(species) == 0):
          lspecies = 'All'
        elif (len(species) == 1):
          lspecies = species[0]
        else:
          raise Exception, "Selection with several species is not available for online CDMS"
      else:
        raise Exception, "Unexpected kind of argument: "+repr(species)

      if self.online:
         lines = self.__post(fmin, fmax, lspecies, energy, einstein)
      else:
         raise Exception, "Offline in cdms instance"

      return lines

   def part_function(self, species, origin, dbsource):
      """
      Returns the partition function at different temperatures

      This function fetches a text file (in the format used in the CDMS
      database) containing the partition function of the given species
      for different temperatures and then parse it. The file content is
      kept in memory to avoid re-fetching if the function is called
      several times.

      Arguments:
      species -- the species name

      """

      global partfuncsCached

      if origin.lower() != self.name:  # Case-insensitive
         raise ValueError, "Got %s, but want cdms as origin for partfunc in cdms" % origin

      if dbsource != self.name:
         raise ValueError, "Got %s, but want cdms as dbsource for partfunc in cdms" % dbsource

      temp = [1000., 500., 300., 225., 150., 75., 37.5, 18.75, 9.375]

      try:
         partfuncsCached
      except NameError:
         # TODO(mpl): shouldn't partfunc_url be .encode()ed as well?
         # -> causes problem with timeout, wtf. will investigate later.
         #f =  urllib2.urlopen(partfunc_url.encode('utf-8'))
         f =  urllib2.urlopen(partfunc_url)
         partfuncsCached = f.readlines()
         f.close()

      temperature = []
      partition_function = []

      for l in partfuncsCached:
         l = l.decode('utf-8')
   
         if l[0] == "<":
            continue
         try:
            spec = l[7:28].strip()
            #print 'spec=',spec,'species=',species[7:]
            
            if spec == "":
               continue
            if spec == species[7:].strip():
               print 'partition function found'
               field = l[40:].split()
               for i in range(len(temp)):
                  if field[i] == "---":
                     continue
                  temperature.append(temp[i])
                  partition_function.append(10**float(field[i]))
         except:
            continue

      if partition_function == []:
         raise db.NotFoundError, "No partition function found for %s." % species

      return temperature, partition_function

default = Cdms(url = "http://www.astro.uni-koeln.de/cgi-bin/cdmssearch",
          cache_file = "~/.gag/scratch/cdms.db", protocol = "cdms_post",
          online = True, name = "cdms")
