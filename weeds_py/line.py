# line.py -- Classes and methods for spectral lines

class line:
   """
   Spectral line
   """

   def __init__(self):
      self.species = ''
      self.frequency = 0.
      self.err_frequency = 0.
      self.einstein_coefficient = 0.
      self.upper_level = level()
      self.lower_level = level()
      self.origin = ''   # initial origin of the line (jpl, cdms...)
      self.dbsource = '' # last database this line has been read from
      # we need to keep that info as well, for when calling part_func, while inserting lines. I don't like it though.
      self.prev = ''
      self.date = ''

   def __repr__(self):
      return "%-16s | %12.3f | %7.3f | %6.1f | %s -- %s" % \
      (self.species, self.frequency, self.err_frequency,
      self.upper_level.energy * 1.44,
      self.upper_level.quantum_numbers, self.lower_level.quantum_numbers)

class level:
   """
   Energy level
   """

   def __init__(self):
      self.energy = 0.
      self.statistical_weight = 0.
      self.quantum_numbers = ''

if __name__ == "__main__":
   l = line()
   print l
