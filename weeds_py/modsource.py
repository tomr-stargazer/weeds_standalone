# modsource.py -- Model the emission of a source at the LTE
# 
# This file is part of Weeds.

from numpy import *
import sys
#import pyclass
from consts import *
import cdms
from db import blankPartfunc
from sicparse import OptionParser
import copy
from scipy.interpolate import interp1d

class component:
    pass

def readmdl(filename):
    """
    Read the source model

    Arguments
    filename -- name of the source model (".mdl")
    
    """

    components = []
    linenumber = 0

    try:
        f = open(filename)
    except:
        raise Exception("Can't open %s" % filename)

    # Set a parser for the optional fields
    parser = OptionParser()
    parser.add_option("-c", "--cache", action="store_false", dest="online",
                      default=True) # unused
    parser.add_option("-a", "--absorption", action="store_true", dest="absorption",
                      default=False)
    parser.add_option("-p", "--partfunc", dest = "partfunc", nargs = 1,
                      type = "float", default = None)

    try:
        for line in f.readlines():
            if len(line.strip()) == 0:
                continue
            linenumber = linenumber + 1
            if line[0] in ["#", "!"]:
                continue
            c = component()

            # Get the species name
            if line[0] == '"':
                c.species = line.split('"')[1]
                line = line.split('"', 3)[2]
            else:
                c.species = line.split(None, 1)[0]
                line = line.split(None, 1)[1]

            # Get other mandatory fields
            field = line.split(None)
            c.Ntot = float(field[0])
            c.Tex = float(field[1])
            c.theta = float(field[2])
            c.v_off = float(field[3])
            c.delta_v = float(field[4])
 
            # Get optional fields
            if len(field) > 5:
                (opts, args) = parser.parse_args(field[5:])
            else:
                (opts, args) = parser.parse_args([]) # to set default values
            if len(args) == 1:
                c.origin = args[0]
            else:
                c.origin = 'All'
            if len(args) > 1:
                raise ValueError
            if not(opts.online):
                pyclass.message(pyclass.seve.w, "MODSOURCE", "/CACHE option is obsolete (ignored).")
            c.absorption = opts.absorption
            c.partfunc = opts.partfunc # log10 of the partition function
            c.keep_opacity = False

            components.append(c)

    except Exception:
        raise Exception("Incorrect input on line %i of %s" % (linenumber, filename))

    return components

def J(T, freq):
    """
    Returns the radiation temperature

    This function returns the radiation temperature corresponding to
    the given kinetic temperature at the given frequency. See Eq. 1.28
    in the Tools of Radio Astronony by Rohlfs & Wilson.

    Arguments:
    T    -- kinetic temperature, in K
    freq -- frequency, in MHz

    """

    J = planck_constant * freq * 1e6 / boltzmann_constant \
            / (exp((planck_constant * freq * 1e6) \
                               / (boltzmann_constant * T)) - 1)

    return J


def Planck_funct(T, freq):
    """
    Return a blackbody intensity in Jy 

    Arguments:
    T [K]
    freq [MHz]

    """

    kk = 1.3807e-16 # Bolzmann's constant [erg/K]
    hh = 6.6262e-27 # Planck's constant [erg.s]
    cc  = 2.99792458e10  # Light speed [cm/s]

    
    freq = freq*1e6 # MHz->Hz

    factor = 2.*hh/cc/cc*1e23 # Jy

    bt = factor*freq**3/(exp(hh*freq/kk/T)-1.)

    return bt

# TSR: I was told that K. Zhang wrote this function.
def FindFreqStep(components,fmin):
    delta_v = []
    for c in components:
        delta_v.append(c.delta_v)
    min_delta_v = min(abs(array(delta_v)))
    min_delta_f = min_delta_v * 1e3 / speed_of_light * fmin # MHz
    model_freq_step = min_delta_f / 10.

    return model_freq_step


def getLines(cdmsobject,fmin,fmax,species,origin,energy = -1, einstein=-1):
    
    lines = cdmsobject.search(fmin,fmax,species=species,origin=origin)

    return lines

def getPartitionfuc(cdmsobject,species,Tex):
    """get partition function, given species name and excitation temperature"""
    t_dummy, part_dummy = cdmsobject.part_function(species,'cdms','cdms')
    f = interp1d(log(t_dummy),log(part_dummy))
    return exp(f(log(Tex)))

def modsource(components, fmin, fmax, freq_step = None, \
              theta_tel = None, background = 2.7, \
              verbose = False,extra_result = False):
    """
    Model the emission of a given source at the ETL

    """
    if freq_step == None:
        freq_step = FindFreqStep(components,fmin)

    freq = arange(fmin, fmax, freq_step)  

    # Compute the antenna temperature and opacity over the entire
    # frequency range.  See Maret et al. (2010, in prep.) for the
    # formula used.

    tb_grand_tot = zeros(len(freq))  # All components, species and lines
    # TSR: I was told that K. Zhang added the `intensity_grand_tot` parameter.
    intensity_grand_tot = zeros(len(freq)) 
    
    tb_species = zeros((len(freq),len(components)))
    keep_opacity_flag = False
    i = 0

    ## a cdms object

    cdmsobject = cdms.Cdms(url = "http://www.astro.uni-koeln.de/cgi-bin/cdmssearch",
          cache_file = "~/.gag/scratch/cdms.db", protocol = "cdms_post",
          online = True, name = "cdms")

    
    for c in components:
        #print 'computing for species %s' %(c.species)
        
        lines = getLines(cdmsobject,fmin,fmax,c.species,c.origin,-1,-1)
        

        if len(lines) == 0:
            print "No %s lines found in the frequency range" % (c.species)
            i = i+1
            continue
        else:
            print " %i %s lines found in the frequency range" % (len(lines), c.species)

        
            partitionfunc= getPartitionfuc(cdmsobject,c.species,c.Tex)

            # Compute the total opacity for that species

            tau_tot = zeros(len(freq))
            for l in lines:

                # Line profile function
                freq_off = -c.v_off * 1e3 / speed_of_light * l.frequency # MHz
                sigma = l.frequency / (speed_of_light * sqrt(8 * log(2))) \
                        * c.delta_v * 1e3 * 1e6 # Hz
                phi = 1 / (sigma * sqrt(2 * pi)) * exp (-((freq - l.frequency - freq_off) \
                                                          * 1e6)**2 / (2 * sigma**2))

                # Line opacity
                tau = speed_of_light**2 / (8 * pi * (freq * 1e6)**2) * l.einstein_coefficient \
                      * c.Ntot * 1e4 * l.upper_level.statistical_weight \
                      * exp(-l.upper_level.energy / c.Tex) \
                      / partitionfunc * (exp(planck_constant * l.frequency * 1e6 \
                                                                 / (c.Tex * boltzmann_constant))-1) * phi

                # Opacity at line center
                l.tau0 = max(tau)
                
                tau_tot = tau_tot + tau
                
                
                if c.keep_opacity:
                    tau_kept = tau_tot
                    keep_opacity_flag = True
        
                    

              
        # Compute the antenna temperature for that species

        Tbg = background
        eta_source = c.theta**2 / (theta_tel**2 + c.theta**2)
        tb_tot = eta_source * (J(c.Tex, freq) - J(Tbg, freq)) * (1 - exp(-tau_tot))
        
        if not(c.absorption):
            tb_grand_tot = tb_grand_tot + tb_tot
            solid_angle = pi*c.theta**2/(206265.*206265.)
            intensity_grand_tot = Planck_funct(c.Tex,freq)*(1.-exp(-tau_tot))*solid_angle+intensity_grand_tot
            
        else:
            tb_grand_tot = tb_grand_tot * exp(-tau_tot) + tb_tot
            

        tb_species[:,i] = tb_tot
        i = i+1    

        
    # K. Zhang added this `extra_result` functionality.        
    if extra_result == True:
        return freq, tb_grand_tot,tb_species,tau_tot,intensity_grand_tot
    else:
        return freq, tb_grand_tot,tb_species


