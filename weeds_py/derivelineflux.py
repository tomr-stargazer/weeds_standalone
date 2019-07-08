from numpy import *
from .consts import *
from scipy.integrate import simps
from .modsource import J
from scipy.interpolate import interp1d
from astropy.io import ascii
from . import cdms
from . import modsource
import scipy.optimize as optimization
import matplotlib.pyplot as plt



def CalculateLineFlux(para,lines = None, fwhm = None ,part=None):
    
    """calculate line fluxes of a given list of lines
         para: N_tot (cm^-2), Tex (K)
         lines:spectral information list from cdmsobject.search
         fwhm: a list of fwhm in km/s
         part: partion function from cdmsobject.part_function
         
         Output:
         an numpy array of line flux in unit of K. km/s
         KZ 03/10/2016"""
    
    ntot = para[0]
    tex  = para[1]
    f_dum  = interp1d(log(part[0]),log(part[1]))
    partitionfunc = exp(f_dum(log(tex)))
    
    nline = len(lines)
    line_flux = zeros(nline)
    
    
    for i in range(nline):
        l = lines[i][0]
        freq_fwhm = fwhm[i]*1e3/speed_of_light*l.frequency
        
        freq = arange(l.frequency-2.*freq_fwhm,l.frequency+2.*freq_fwhm,freq_fwhm/20.)
        v_kms_grid = (freq-l.frequency)/l.frequency*speed_of_light*1e-3
        
        sigma = l.frequency / (speed_of_light * sqrt(8 * log(2))) \
                * fwhm[i] * 1e3 * 1e6 # Hz
            
        phi = 1 / (sigma * sqrt(2 * pi)) * exp (-((freq - l.frequency) \
                                                          * 1e6)**2 / (2 * sigma**2))
                        # Line opacity
        tau = speed_of_light**2 / (8 * pi * (freq * 1e6)**2) * l.einstein_coefficient \
                      * ntot * 1e4 * l.upper_level.statistical_weight \
                      * exp(-l.upper_level.energy / tex) \
                      / partitionfunc * (exp(planck_constant * l.frequency * 1e6 \
                                                                 / (tex * boltzmann_constant))-1) * phi
        tb = J(tex, freq)*(1.-exp(-1.*tau)) # assumed no beam dilution
        #plt.plot(v_kms_grid,tb)
                    
        line_flux[i] = simps(tb,v_kms_grid)
        
    return line_flux



def NtotTexFittingFunc(data,species_ind=48501,species = '048501 SO, v=0',\
                       p0=[1e14,50.] ):
    """Find out best fitting N_tot and Tex a species"""

    # observational results
    ind = data['col1'] ==str(species_ind)
    freq = data['col3'][ind]
    fwhm = data['col8'][ind]
    flux = data['col10'][ind]
    ef   = data['col11'][ind]
    
    # spectral information and partition function
    cdmsobject= cdms.Cdms(url = "https://cdms.astro.uni-koeln.de/cgi-bin/cdmssearch",
          cache_file = "~/.gag/scratch/cdms.db", protocol = "cdms_post",
          online = True, name = "cdms")
    part = cdmsobject.part_function(species,'cdms','cdms')
    
    lines = []
    for ifreq in freq:
        dummy = cdmsobject.search(ifreq-0.5,ifreq+0.5,species=species,origin='All')
    
        lines.append(dummy)

    # a wrapper function
    def mixCalculateLineFlux(x,ntot,tex):
        para = [ntot,tex]
        y = CalculateLineFlux(para,lines=lines,fwhm=fwhm,part=part)
    return y

    popt, pcov = optimization.curve_fit(mixCalculateLineFlux, x, flux,sigma=ef,p0=p0)

    if plot_rotation_diagram == True:
        nline = len(lines)
        eup = zeros(nline)
        gup = zeros(nline)
        acoef = zeros(nline)
        for i in range(nline):
            l = lines[i][0]
            eup[i] = l.upper_level.energy
            gup[i] = l.upper_level.statistical_weight
            acoef[i] = l.einstein_coefficient
        plt.semilogy(eup,flux/gup/acoef/freq,'o')
        plt.show()
