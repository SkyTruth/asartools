#!/usr/bin/env python


# This document is part of asartools
# https://github.com/skytruth/asartools


# =========================================================================== #
#
#  The MIT License (MIT)
#
#  Copyright (c) 2014 SkyTruth
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
# =========================================================================== #


"""
A wrapper for the GDAL commands needed to convert N1 formatted
ENVISAT ASAR data to a GDAL supported raster format
"""


__license__ = '''
The MIT License (MIT)

Copyright (c) 2014 SkyTruth

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''


import os
import sys
import subprocess
from os.path import *


#/* ======================================================================= */#
#/*     Build Information
#/* ======================================================================= */#

__author__ = 'Kevin Wurster'
__version__ = '0.1'
__release__ = '2014-06-13'
__package__ = 'asartools'
__docname__ = basename(__file__)
__source__ = 'https://github.com/skytruth/asartools'


#/* ======================================================================= */#
#/*     Define print_usage() Function
#/* ======================================================================= */#

def print_usage():

    """
    Print commandline usage information

    :return: returns 1 for for exit code purposes
    :rtype: int
    """

    print("""
Usage: %s --help-info [options] input.N1 output.tif


Options:

  --overwrite  ->  Overwrite output files if they exist
  --print  ->  Print GDAL utility commands instead of executing

  --iscale min max  ->  gdal_translate src_min/src_max for -scale flag
  --oscale min max  ->  gdal_translate dst_min/dst_max for -scale flag

  --gdalinfo=str  ->  Explicitly define which utility to use
  --gdalwarp=str  ->  Explicitly define which utility to use
  --gdal_translate=str  ->  Explicitly define which utility to use
    """ % __docname__)

    return 1


#/* ======================================================================= */#
#/*     Define print_help() Function
#/* ======================================================================= */#

def print_help():

    """
    Print more detailed help information

    :return: returns 1 for for exit code purposes
    :rtype: int
    """

    print('''
Help: {0}" % __docname__)
------{1}
This utility executes a string of GDAL utility commands to convert an raster
stored as an N1 to a losslessly compressed GeoTiff.  Use the '--print' flag
to see exactly which commands are executed but the general workflow is as
follows:


  gdalinfo

    Extract the raster's min/max values


  gdalwarp

    Define an SRS and apply GCP's.  Note that the output from this command is
    a temporary intermediate VRT file that is immediately deleted after all
    commands have executed.


  gdal_translate

    Convert to GeoTiff, set data type to 'Byte', scale values, and
    losslessly compress


By default, the gdal_translate command's src_min/max scale values are set to
the input file's min/max values and the dst_min/max scale values are set to
0 255.  The user has the option to overwrite these values with the --iscale
and --oscale flags.


  Scale input values between 100 and 200 to 50 to 100

    --iscale 100 200 --oscale 50 100


  Same command, but detect the input raster's minimum value

    --iscale %detectmin 200 --oscale 50 100


  Detect the input raster's maximum value

    --iscale 100 %detectmax --oscale 50 100
'''.format(__docname__, '-' * len(__docname__)))

    return 1


#/* ======================================================================= */#
#/*     Define print_license() Function
#/* ======================================================================= */#

def print_license():

    """
    Print licensing information

    :return: returns 1 for for exit code purposes
    :rtype: int
    """

    print(__license__)

    return 1


#/* ======================================================================= */#
#/*     Define print_help_info() Function
#/* ======================================================================= */#

def print_help_info():

    """
    Print a list of help related flags

    :return: returns 1 for for exit code purposes
    :rtype: int
    """

    print("""
Help flags:
  --help    -> More detailed description of this utility
  --usage   -> Arguments, parameters, flags, options, etc.
  --version -> Version and ownership information
  --license -> License information
    """)

    return 1


#/* ======================================================================= */#
#/*     Define print_version() Function
#/* ======================================================================= */#

def print_version():

    """
    Print the module version information

    :return: returns 1 for for exit code purposes
    :rtype: int
    """

    print("""
%s version %s - released %s
    """ % (__docname__, __version__, __release__))

    return 1


#/* ======================================================================= */#
#/*     Define main() Function
#/* ======================================================================= */#

def main(args):

    """
    Commandline logic

    :param args: arguments gathered from the commandline
    :type args: list

    :return: returns 0 on success and 1 on error
    :rtype: int
    """

    #/* ======================================================================= */#
    #/*     Defaults
    #/* ======================================================================= */#

    # GDAL utilities
    gdalinfo_utility = 'gdalinfo'
    gdalwarp_utility = 'gdalwarp'
    gdal_translate_utility = 'gdal_translate'

    # GDAL utility options
    gdalinfo_options = " -stats -nofl -noct -norat -nomd -nogcp -mm %infile "
    gdalwarp_options = " -of VRT -tps -s_srs EPSG:4326 -t_srs EPSG:4326 %infile %outfile "
    gdal_translate_options = " -of GTiff -ot Byte -scale %detectmin %detectmax %oscalemin %oscalemax -co SPARSE_OK=True -co INTERLEAVE=BAND -co BIGTIFF=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 %infile %outfile "

    # gdaltranslate scale defaults
    gdal_translate_default_oscalemin = '0'
    gdal_translate_default_oscalemax = '255'

    # Additional options
    print_mode = False
    overwrite_mode = False

    #/* ======================================================================= */#
    #/*     Containers
    #/* ======================================================================= */#

    # Input/output files
    input_n1 = None
    output_raster = None
    intermediary_vrt = None

    #/* ======================================================================= */#
    #/*     Parse Arguments
    #/* ======================================================================= */#

    i = 0
    arg_error = False
    while i < len(args):

        try:
            arg = args[i]

            # Help arguments
            if arg in ('--help-info', '-help-info', '--helpinfo', '-help-info'):
                return print_help_info()
            elif arg in ('--help', '-help', '--h', '-h'):
                return print_help()
            elif arg in ('--usage', '-usage'):
                return print_usage()
            elif arg in ('--version', '-version'):
                return print_version()
            elif arg in ('--license', '-usage'):
                return print_license()

            # Explicitly define GDAL utilities
            elif '--gdalinfo=' in arg:
                i += 1
                gdalinfo_utility = arg.split('=', 1)[1]
            elif '--gdalwarp=' in arg:
                i += 1
                gdalwarp_utility = arg.split('=', 1)[1]
            elif '--gdal_translate=' in arg:
                i += 1
                gdal_translate_utility = arg.split('=', 1)[1]

            # Processing options
            elif arg in ('--iscale', '-iscale'):
                i += 1
                while i < len(args) and args[i] != '-':
                    if '%detectmin' in gdal_translate_options:
                        gdal_translate_options.replace('%detectmin', args[i])
                    elif '%detectmax' in gdal_translate_options:
                        gdal_translate_options.replace('%detectmax', args[i])
                    else:
                        break  # Helps to detect bad parameters
                    i += 1
            elif arg in ('--oscale', '-oscale'):
                i += 1
                while i < len(args) and args[i] != '-':
                    if '%oscalemin' in gdal_translate_options:
                        gdal_translate_options.replace('%oscalemin', args[i])
                    elif '%oscalemax' in gdal_translate_options:
                        gdal_translate_options.replace('%oscalemax', args[i])
                    else:
                        break  # Helps to detect bad parameters
                    i += 1

            # Additional options
            elif arg in ('--print', '-print'):
                i += 1
                print_mode = True
            elif arg in ('--overwrite', '-overwrite'):
                i += 1
                overwrite_mode = True

            # Positional arguments and errors
            else:

                # Catch input N1 file
                if input_n1 is None:
                    i += 1
                    input_n1 = normpath(arg)

                # Catch output raster
                elif output_raster is None:
                    i += 1
                    output_raster = normpath(arg)

                # Errors
                else:
                    i += 1
                    arg_error = True
                    print("ERROR: Unrecognized argument: %s" % arg)

        # An argument with parameters likely didn't iterate 'i' properly
        except IndexError:
            i += 1
            arg_error = True
            print("ERROR: An argument has invalid parameters")

    #/* ======================================================================= */#
    #/*     Validate
    #/* ======================================================================= */#

    # Define intermediary VRT file path
    if output_raster is not None:
        intermediary_vrt = output_raster.split('.', -1)[0] + '.vrt'

    bail = False

    # Check arguments
    if arg_error:
        bail = True
        print("ERROR: Problem parsing arguments")

    # Check input file
    if input_n1 is None:
        bail = True
        print("ERROR: Need an input file")
    elif not os.access(input_n1, os.R_OK):
        bail = True
        print("ERROR: Input file doesn't exist or needs read access: %s" % input_n1)

    # Check output file
    if output_raster is None:
        bail = True
        print("ERROR: Need an output file")
    elif not overwrite_mode and isfile(output_raster):
        bail = True
        print("ERROR: Overwrite=%s and output file exists: %s" % (str(overwrite_mode), output_raster))

    # Check intermediate file
    if intermediary_vrt is None:
        bail = True
        print("ERROR: Can't create intermediary file path without output file")
    elif not overwrite_mode and isfile(intermediary_vrt):
        bail = True
        print("ERROR: Overwrite=%s and intermediary file exists: %s" % (str(overwrite_mode), intermediary_vrt))

    # Check output directory
    elif not os.access(normpath(dirname(output_raster)), os.W_OK):
        bail = True
        print("ERROR: Need write access: %s" % normpath(dirname(output_raster)))

    # Exit if an problem was found
    if bail:
        return 1

    #/* ======================================================================= */#
    #/*     Execute Command: gdalinfo
    #/* ======================================================================= */#

    # gdalinfo - get min/max
    if '%detectmin' in gdal_translate_options or '%detectmax' in gdal_translate_options:

        # Call gdalinfo to get the min/max values for input raster
        print("Getting min/max for input file ...")

        # Add the input file to the command
        gdalinfo_options = gdalinfo_options.replace('%infile', input_n1)

        # Build and call command
        command = [gdalinfo_utility] + gdalinfo_options.split()
        p = subprocess.Popen(command, stdout=subprocess.PIPE)
        output, err = p.communicate()

        # Pull the min/max raster values out of the gdalinfo output
        min_max_line = [line.strip() for line in output.split(os.linesep) if 'Minimum=' in line and 'Maximum=' in line][0]
        iscale_min, iscale_max = min_max_line.split()[:2]
        iscale_min = iscale_min.replace('Minimum=', '')
        iscale_min = iscale_min.replace(',', '')
        iscale_max = iscale_max.replace('Maximum=', '')
        iscale_max = iscale_max.replace(',', '')

        print("  Min = %s" % iscale_min)
        print("  Max = %s" % iscale_max)

        # Make replacements
        if '%detectmin' in gdal_translate_options:
            gdal_translate_options = gdal_translate_options.replace('%detectmin', iscale_min)
        if '%detectmax' in gdal_translate_options:
            gdal_translate_options = gdal_translate_options.replace('%detectmax', iscale_max)

    #/* ======================================================================= */#
    #/*     Execute Command: gdalwarp
    #/* ======================================================================= */#

    # gdalwarp - reproject to a VRT in order to write GCP information
    print("Reprojecting ...")
    if overwrite_mode and isfile(intermediary_vrt):
        os.remove(intermediary_vrt)

    # Add input/output files
    gdalwarp_options = gdalwarp_options.replace('%infile', input_n1).replace('%outfile', intermediary_vrt)

    # Build and call command
    command = [gdalwarp_utility] + gdalwarp_options.split()
    if print_mode:
        print(" ".join(command))
    else:
        subprocess.call(command)

    #/* ======================================================================= */#
    #/*     Execute Command: gdal_translate
    #/* ======================================================================= */#

    # gdal_translate - read VRT and write GTiff with the appropriate scale
    print("Writing final output file ...")

    # Adjust command
    if '%oscalemin' in gdal_translate_options:
        gdal_translate_options = gdal_translate_options.replace('%oscalemin', gdal_translate_default_oscalemin)
    if '%oscalemax' in gdal_translate_options:
        gdal_translate_options = gdal_translate_options.replace('%oscalemax', gdal_translate_default_oscalemax)

    # Add input/output files
    gdal_translate_options = gdal_translate_options.replace('%infile', intermediary_vrt).replace('%outfile', output_raster)

    # Build and call command
    command = [gdal_translate_utility] + gdal_translate_options.split()
    if print_mode:
        print(" ".join(command))
    else:
        subprocess.call(command)

    #/* ======================================================================= */#
    #/*     Cleanup
    #/* ======================================================================= */#

    # Delete intermediary file if it exists
    if isfile(intermediary_vrt):
        print("Deleting intermediary file: %s" % intermediary_vrt)
        os.remove(intermediary_vrt)

    # Success
    print("Done.")
    return 0


#/* ======================================================================= */#
#/*     Command line execution
#/* ======================================================================= */#

if __name__ == '__main__':

    # Remove script name and give the rest to main
    if len(sys.argv) > 1:
        sys.exit(main(sys.argv[1:]))

    # Didn't get enough arguments - print usage
    else:
        sys.exit(print_usage())
