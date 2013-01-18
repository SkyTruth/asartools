# asar-tools.py

from httplib2 import Http
from urlparse import urlparse, urljoin
from BeautifulSoup import BeautifulSoup
import urllib2
import shutil
import logging
import re
import os
from string import Template
from tempfile import mkdtemp
from optparse import OptionParser
from datetime import datetime, timedelta   
import psycopg2
from psycopg2.extras import RealDictCursor, register_uuid

from S3.Exceptions import *
from S3.S3 import S3
from S3.Config import Config
from S3.S3Uri import S3Uri

import settings


class GeoDatabase:
    
    def __init__(self):
        self.host = settings.GEO_DB_HOST  
        self.user = settings.GEO_DB_USER
        self.passwd = settings.GEO_DB_PASS
        self.dbname = settings.GEO_DB_DATABASE
        self.db = None
        psycopg2.extras.register_uuid()
        
    def connect (self):        
        try:
            self.db = psycopg2.connect (
                host = self.host, 
                user = self.user, 
                password = self.passwd,
                database = self.dbname
                )
            self.db.autocommit = True    
            logging.info ("Connected to database %s" % self.host)
            logging.debug ("  user: %s database %s" % (self.user, self.dbname))
        except psycopg2.Error, e:
            self.db = None
            logging.error ("Unable to connect to database: Error %d: %s" % 
                (e.args[0], e.args[1]))
            raise 
    
    def cursor (self):
        return self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def exec_sql (self, sql, params = None):
        c = self.db.cursor() 
        if params:
            c.execute(sql, params)
        else:
            c.execute(sql)
        return c.rowcount
                
    def item_exists (self, table, match_fields):
        where_sql = ' and '.join(["%s='%s'" %(k,v) for k,v in match_fields.items()])
        c = self.db.cursor() 
        c.execute('select * from %s where %s' % (table, where_sql))
        return c.rowcount > 0

    def load_item (self, table, match_fields):
        return self.load_items (table, match_fields, limit=1)

    def load_items (self, table, match_fields, limit = None):
        where_sql = ' and '.join(["%s=%%s" %(k) for k in match_fields.keys()])
        c = self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        limit_sql = ''
        if limit:
            limit_sql = 'limit %s' % limit

        sql = 'select * from %s where %s %s' % (table, where_sql, limit_sql)
        c.execute(sql, match_fields.values())
        return c.fetchall()
        pass
    
    def insert_item (self, table, fields):
        key_str = ','.join(fields.keys())
        value_str = ('%s,' * len(fields.values()))[:-1]

        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table.lower(), key_str, value_str)
        values = fields.values()
        c = self.db.cursor()
#        print c.mogrify(sql, values)
        c.execute (sql, values)
        return c.lastrowid
        

def removeNBSP (str):
    regex = re.compile(re.escape('&nbsp;'), re.IGNORECASE)
    return regex.sub(' ', str)

class ESARollingArchive:
    urls = ['https://oa-es.eo.esa.int/ra/asa/index.php', 
            'https://oa-ks.eo.esa.int/ra/asa/index.php',
            'https://oa-ip.eo.esa.int/ra/asa/index.php' ]
    user = 'asausr'
    password = 'asa1sra'
  
    def __init__(self, debug = False):
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        for url in self.urls:
            p = urlparse (url)
            passman.add_password(None, p.hostname, self.user, self.password)
            
        urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passman), urllib2.HTTPSHandler(debuglevel=1)))
        
    
    def scrape (self):
        records = []
        for url in self.urls:
            records.extend (self.scrape_page(url))
        return records
    
    def clean (self, str):
        return removeNBSP(str).strip()
    
    def parse_date (self, str):
        return datetime.strptime (self.clean(str), '%d-%B-%Y %H:%M')
 
    def parse_name (self, name):
        parts = {}
        parts['acquisition_date'] = datetime.strptime(name[14:29],'%Y%m%d_%H%M%S')
        parts['duration'] = timedelta(seconds = int(name[30:38]))
        parts['orbit'] = int (name[49:54])
        return parts
        
    def scrape_page (self, url):
        logging.info ('Scraping image data from: %s' % url)
        
#        f = open('data/esa_rolling_archive.html', 'r')
        f = urllib2.urlopen(urllib2.Request(url))

        try:
            content = f.read()
        finally:
            f.close()        
        
#        h = Http(".cache", disable_ssl_certificate_validation=True)
#        h.add_credentials(self.user, self.password)
#        resp, content = h.request(url, "GET")
       
        records = []
                
        page = BeautifulSoup (content)
        rows = page.findAll ('table')
        for row in rows:
            fields = row.findAll ('td', {'class': 'list'})
            if len(fields) == 5:
               record = {}
               record['url'] = urljoin(url, fields[0].a['href'])
               record['size_bytes'] = int(self.clean(fields[1].renderContents()))
#               record['archive_date'] = self.parse_date(fields[2].renderContents())
               record['name'] = self.clean(fields[4].i.renderContents())
               record['type'] = 'N1'
               record['source'] = 'ENVISAT.ASAR'

               record.update (self.parse_name (record['name']))

               records.append (record)

        return records        
                   
    # returns true if the download succeeds, else false
    def download_image (self, url, dest_filename):
    
        logging.info ('Downloading %s ...' % url)
        
        # get temp file
        tempdir = os.path.join(mkdtemp (),'')
        tempfile = os.path.join(tempdir, os.path.basename(dest_filename) )
        # use wget to donwload
        cmd = 'wget -q %s --no-check-certificate --http-user=%s --http-password=%s --output-document=%s'

        result = os.system (cmd % (url, self.user, self.password, tempfile))
        # copy from temp file to dest file

        shutil.copy(tempfile,dest_filename)
        
        os.unlink(tempfile)
        os.rmdir (tempdir)            

#        retry = 5
#        
#        while retry > 0:
#            try:
#                r = urllib2.urlopen(urllib2.Request(url))
#            except urllib2.HTTPError as err:
#                logging.error ('Failed downloading url %s with error %s ' % (url, err))
#                return False
#            except urllib2.URLError as err:
#                if err.reason[0] == 10060:
#                    if retry > 1:
#                        logging.warning ('Connection to %s timed out. Retrying...' % (url,))
#                        retry -= 1
#                    else:
#                        logging.error ('Failed to connect to url %s with error %s ' % (url, err))
#                        return False
#                else:    
#                    logging.error ('Failed to connect to url %s with error %s ' % (url, err))
#                    return False
#                
#              
#        try:
#            with open(dest_filename, 'wb') as f:
#                shutil.copyfileobj(r,f)
#        finally:
#            r.close()    
                
        return True       
        

class EnvisatBest:
    tempdir = None
    
    def __init__(self, auto_cleanup = True):
        self.tempdir = os.path.join(mkdtemp (),'')
        self.auto_cleanup = auto_cleanup
        logging.debug ('Using temp dir: %s' %self.tempdir )
    
    def __del__(self):
        # delete all files in the temp directory and then delete the directory
        
        if self.auto_cleanup and self.tempdir:
            logging.info ("Cleaning up temp directory...")
            logging.debug ("  %s" % self.tempdir)
            try:
                for the_file in os.listdir(self.tempdir):
                    file_path = os.path.join(self.tempdir, the_file)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                os.rmdir (self.tempdir)            
            except Exception, e:
                logging.error ("%s" % e)
        self.tempdir=None
           
    #returns full path to ini file    
    def create_ini_file (self,content):
        ini_file = os.path.join(self.tempdir, 'parameters.ini')
        of = open(ini_file, 'w')
        of.write(content)
        of.close
        return ini_file 
        
    def execute (self,ini_file_content):
        ini_file = self.create_ini_file (ini_file_content)
        
        logging.debug ('Executing best with ini file: %s' %ini_file)
        
        # looks like it always returns 0 :(
        result = os.system ('best %s > %s' % (ini_file, os.path.join(self.tempdir, 'best.out')))
        


class Error (Exception):
    pass 


# finds regex patterns by searching a give file line by line
# supply patterns as regex patterns in a dictionary
# returns results in a dictionary using the same keys as supplied with the patterns
# finds the first match for each pattern
class FilePatternMatcher:
    def __init__(self, filename, patterns):
        self.filename = filename
        self.patterns = patterns
        
    # if require_all is true, throws an exception if the end of the file is reached and not all 
    # patterns are matched
    def get_matches (self, require_all = True):
        matches = {}
        
        f = open(self.filename, "r")
        regexes = {}
        for k,v in self.patterns.items():
            regexes[k] = re.compile(v)
            
        for line in f:
            for k in regexes.keys():
                m = regexes[k].findall (line)
                if m:
                    del regexes[k]
                    matches[k] = m[0]
        if regexes and require_all:
            raise Error ('Failed to match all patterns while searching file: %s.\n%s' % (self.filename, regexes.keys()))
            
        return matches
            

class AsarImageFile:
    processor = None
    ini_templates = None
    n1_file = None
    header_txt_file = None
    default_params = None
    sensor_modes = {'IMM': 'Image', 'WSM': 'Wide Swath', 'APM':'Alternating Polarization'}
    
    def __init__ (self, processor, n1_file):
        self.processor = processor    
        self.n1_file = n1_file
        self.ini_templates = self.get_ini_templates()
        self.params = {'n1_file': self.n1_file, 'output_dir': self.processor.tempdir, 'input_dir': self.processor.tempdir}    
        sensor_key = os.path.basename(n1_file)[4:7]
        
        if sensor_key in self.sensor_modes:
            self.params['sensor_mode'] = self.sensor_modes[sensor_key]
        else:
            logging.error ('Unknown sensor mode in: %s' % self.n1_file )
            self.params['sensor_mode'] = 'Unknown'
            

    def extract_header (self):
        logging.info ('Extracting header data...')
        logging.debug ('   from %s' % self.n1_file )
        
        self.processor.execute (self.ini_templates['header'].substitute(self.params))

        file = os.path.join(self.params['output_dir'], 'header.txt')
        
        if not os.path.exists (file):
            raise Error ('Failed to extract header')
            
        self.header_txt_file = file    

    def extract_footprint (self):
        if not self.header_txt_file:
            raise Error ('You must call extract_header() before calling extract_footprint()')

        if not os.path.exists (self.header_txt_file):
            raise Error ('Header text file does not exist at: %s' %self.header_txt_file)
        
        logging.info ('Extracting footprint...')
        logging.debug ('  from header file: %s' % self.header_txt_file)
        
        # ff = first sample first line
        # lf = last sample first line
        # fl = first sample last line
        # ll = last sample last line
        patterns = {
            'pass': '53 Ascending or descending orbit designator\s+PASS="([\w]+)\s*"',
            'ff_lat': '14 Geodetic latitude of the first sample at\s+([+-][\d]+)',
            'ff_lng': '18 East geodetic longitude of the first sam\s+([+-][\d]+)',
            'lf_lat': '24 Geodetic Latitude of the last sample of\s+([+-][\d]+)',
            'lf_lng': '28 East geodetic longitude of the last samp\s+([+-][\d]+)',
            'fl_lat': '32 Geodetic Latitude of the first sample of\s+([+-][\d]+)',
            'fl_lng': '36 East geodetic longitude of the first sam\s+([+-][\d]+)',
            'll_lat': '42 Geodetic Latitude of the last sample of\s+([+-][\d]+)',
            'll_lng': '46 East geodetic longitude of the last samp\s+([+-][\d]+)'
        }        
        
        matcher = FilePatternMatcher (self.header_txt_file, patterns)        
        matches = matcher.get_matches ()
        if matches['pass'] == 'ASCENDING':
            coord_map = {'ff':0, 'lf': 1, 'fl': 3, 'll': 2}
        else:
            coord_map = {'ff':2, 'lf': 3, 'fl': 1, 'll': 0}
        
        corners = [{}, {}, {}, {}]
        for k,v in coord_map.items():
            corners[v]['lat'] = float(matches['%s_lat'%k])/1000000
            corners[v]['lng'] = float(matches['%s_lng'%k])/1000000
        
        matches['corners'] = corners
        
        return matches
        
    def extract_geotiff_footprint (self):
    
        txt_file = os.path.join(self.params['output_dir'], 'gdalinfo.txt')

        if not os.path.exists (txt_file):
            raise Error ('gdalinfo text file does not exist at: %s' % txt_file)
        
        logging.debug ('Extracting geotiff footprint from: %s' % txt_file)

        patterns = {
            'bottom_left_lat':  'Lower Left[\s]*\([\s]*[+\-\d\.]+[\s]*,[\s]*([+\-\d\.]+)[\s]*\)',
            'bottom_right_lat': 'Lower Right[\s]*\([\s]*[+\-\d\.]+[\s]*,[\s]*([+\-\d\.]+)[\s]*\)',
            'top_left_lat':     'Upper Left[\s]*\([\s]*[+\-\d\.]+[\s]*,[\s]*([+\-\d\.]+)[\s]*\)',
            'top_right_lat':    'Upper Right[\s]*\([\s]*[+\-\d\.]+[\s]*,[\s]*([+\-\d\.]+)[\s]*\)',
            'bottom_left_lon':  'Lower Left[\s]*\([\s]*([+\-\d\.]+)[\s]*,[\s]*[+\-\d\.]+[\s]*\)',
            'bottom_right_lon': 'Lower Right[\s]*\([\s]*([+\-\d\.]+)[\s]*,[\s]*[+\-\d\.]+[\s]*\)',
            'top_left_lon':     'Upper Left[\s]*\([\s]*([+\-\d\.]+)[\s]*,[\s]*[+\-\d\.]+[\s]*\)',
            'top_right_lon':    'Upper Right[\s]*\([\s]*([+\-\d\.]+)[\s]*,[\s]*[+\-\d\.]+[\s]*\)'
        }        

#        patterns = {
#            'bottom_left_lat': 'bottom_left_lat =(.*)',
#            'bottom_left_lon': 'bottom_left_lon =(.*)',
#            'bottom_right_lat': 'bottom_right_lat =(.*)',
#            'bottom_right_lon': 'bottom_right_lon =(.*)',
#            'top_left_lat': 'top_left_lat =(.*)',
#            'top_left_lon': 'top_left_lon =(.*)',
#            'top_right_lat': 'top_right_lat =(.*)',
#            'top_right_lon': 'top_right_lon =(.*)'
#        }        
        
        matcher = FilePatternMatcher (txt_file, patterns)        
        matches = matcher.get_matches ()

        north = max (matches['top_left_lat'], matches['top_right_lat'])    
        south = min (matches['bottom_left_lat'], matches['bottom_right_lat'])    
        east = max (matches['bottom_right_lon'], matches['top_right_lon'])    
        west = min (matches['bottom_left_lon'], matches['top_left_lon'])    

        corners = [
            {'lat':south, 'lng':west}, 
            {'lat':south, 'lng':east}, 
            {'lat':north, 'lng':east}, 
            {'lat':north, 'lng':west} 
            ]
        return corners
        
#        patterns = [
#            {'pattern': 'bottom_left_lat =', 'corner': 0, 'coord': 'lat'}, 
#            {'pattern': 'bottom_left_lon =', 'corner': 0, 'coord': 'lng'}, 
#            {'pattern': 'bottom_right_lat =',  'corner': 1, 'coord': 'lat'}, 
#            {'pattern': 'bottom_right_lon =', 'corner': 1, 'coord': 'lng'}, 
#            {'pattern': 'top_left_lat =', 'corner': 3, 'coord': 'lat'}, 
#            {'pattern': 'top_left_lon =', 'corner': 3, 'coord': 'lng'}, 
#            {'pattern': 'top_right_lat =',  'corner': 2, 'coord': 'lat'}, 
#            {'pattern': 'top_right_lon =', 'corner': 2, 'coord': 'lng'} 
#            ]
        
    
    def extract_fullres (self, bbox = None):
    
        logging.debug ('Extracting full res data from: %s' % self.n1_file )
        params = self.params.copy()
        if bbox:
            logging.debug ('Using bounding box: %s' % bbox)
            ini_template = 'fullres_clipped'
            params.update(bbox)
        else:
            ini_template = 'fullres'
    
        self.processor.execute (self.ini_templates[ini_template].substitute(params))
        
        if not os.path.exists (os.path.join(self.params['output_dir'], 'fullres.XTs')):
            raise Error ('BEST Failed to extract full res')


    def geocorrect (self):
        logging.debug ('geocorrecting from extracted full res image')
        self.processor.execute (self.ini_templates['geocorrect'].substitute(self.params))
        if not os.path.exists (os.path.join(self.params['output_dir'], 'geocorrect.GRf')):
            raise Error ('BEST Failed to geocorrect image')

    def adjust_gain (self):
        logging.debug ('Adjusting gain from geocorrected image')
        self.processor.execute (self.ini_templates['adjust_gain'].substitute(self.params))
        if not os.path.exists (os.path.join(self.params['output_dir'], 'gain.GCi')):
            raise Error ('BEST Failed to produce gain adjusted image')
        
    def extract_geotiff (self, dest_file):
        logging.info ('Extracting geotiff to: %s' % dest_file)
        params = self.params.copy()
        
        abs_file = os.path.abspath(dest_file)
        
        #params['output_dir'] = os.path.join (os.path.dirname(abs_file), '')
        #params['output_file'] = os.path.basename(abs_file)
        
        self.processor.execute (self.ini_templates['geotiff'].substitute(params))
        
        geotiff = os.path.join(params['output_dir'], 'geotiff.tif')
        if not os.path.exists (geotiff):
            raise Error ('BEST Failed to export geotiff')
        
        if os.path.exists (dest_file):
            os.unlink (dest_file)
        os.system ('gdalwarp -t_srs EPSG:4326 -r cubic -co COMPRESS=LZW %s %s' % (geotiff, dest_file))
        if not os.path.exists (dest_file):
            raise Error ('GDAL Warp failed to re-project geotiff')
            
        gdalinfo_file = os.path.join(params['output_dir'], 'gdalinfo.txt')
        os.system ('gdalinfo %s > %s' % (dest_file, gdalinfo_file))
        if not os.path.exists (gdalinfo_file):
            raise Error ('GDALINFO failed to extract geotiff metadata')

    def extract_quicklook (self, dest_file):
    
        if not self.header_txt_file:
            raise Error ('You must call extract_header() before calling extract_quicklook()')

        if not os.path.exists (self.header_txt_file):
            raise Error ('Header text file does not exist at: %s' %self.header_txt_file)
    
        logging.info ('Extracting quicklook...')
        logging.debug ('  %s' % dest_file)
        params = self.params
        
        abs_file = os.path.abspath(dest_file)
        
#        params['output_dir'] = os.path.join (os.path.dirname(abs_file), '')
#        params['output_file'] = os.path.basename(abs_file)
        self.processor.execute (self.ini_templates['quicklook'].substitute(self.params))
        
        ql_file = os.path.join(self.processor.tempdir, 'ql.tif')
        
        if not os.path.exists(ql_file):
            raise Error ('Error: BEST failed to extract quicklook image')

        shutil.copy(ql_file,abs_file)
    
    def get_ini_templates (self):
        templates = {}
        
        templates['header'] = Template("""
[HEADER ANALYSIS]
Input Media Path = "$n1_file"
Input Media Type = "disk"
Sensor Id = "ASAR"
Sensor Mode = "$sensor_mode"
Product Type = "MR"
AP Dataset = 1
Data Format = "ENVISAT"
Source Id = "esp"
Number Of Volumes = 1
Output Dir = "$output_dir"
Annotation File = "header"
Header Analysis File = "header"
Acknowledge Mount = 'N'
""")


        templates['fullres_clipped'] = Template ("""
[FULL RESOLUTION]
Input Media Path = "$n1_file"
Input Media Type = "disk"
Input Dir = "$input_dir"
Output Dir = "$output_dir"
Header Analysis File = "header.HAN"
Output Image = "fullres"
Sensor Id = "ASAR"
Sensor Mode = "$sensor_mode"
Product Type = "MR"
Coordinate System = "LATLON"
Top Left Corner = $bbox_lat1,$bbox_lng1
Bottom Right Corner = $bbox_lat2,$bbox_lng2        
Acknowledge Mount = 'N'
""")        

        templates['fullres'] = Template ("""
[FULL RESOLUTION]
Input Media Path = "$n1_file"
Input Media Type = "disk"
Input Dir = "$input_dir"
Output Dir = "$output_dir"
Header Analysis File = "header.HAN"
Output Image = "fullres"
Sensor Id = "ASAR"
Sensor Mode = "$sensor_mode"
Product Type = "MR"
Acknowledge Mount = 'N'
""")        

        templates['geocorrect'] = Template ("""
[IMAGE GEO-CORRECTION]
Input Dir = "$input_dir"
Output Dir = "$output_dir"
Input Image = "fullres.XTs"
Output Image = "geocorrect"
Interpolation Mode = "CUBIC CONVOLUTION"        
""")        
        templates['adjust_gain'] = Template ("""
[GAIN CONVERSION]
Input Dir = "$input_dir"
Output Dir = "$output_dir"
Input Image = "geocorrect.GRf"
Output Image = "gain"
Min Percentage = 1.0
Max Percentage = 99.0
""")        
        templates['geotiff'] = Template ("""
[GEO-TIFF GENERATION]
Input Dir = "$input_dir"
Output Dir = "$output_dir"
Input Image = "gain.GCi"
Output Image = "geotiff"
""")        

        templates['quicklook'] = Template ("""
[QUICK LOOK]
Input Media Path = "$n1_file"
Input Media Type = "disk"
Input Dir = "$input_dir"
Output Dir = "$output_dir"
Header Analysis File = "header.HAN"
Output Quick Look Image= "ql"
Output Grid Image = "qlg"
Quick Look Presentation = "GEOGRAPHIC"
Number of Grid Lines = 8 ,8
Output Image Size = 800 ,0
Window Sizes = 3 ,3
Grid Type = "LATLON"
Grid Drawing Mode = "transparent"
Acknowledge Mount = 'N'
""")        

        return templates
	
class S3Loader:
    s3 = None
    
    def __init__(self, config=settings.S3_CONFIG_FILE):
        self.s3 = S3(Config(config))
        
    def put_file (self, filename, s3_uri):
        self.s3.object_put (filename, S3Uri(s3_uri))
        return s3_uri.replace('s3://satimage', 'http://satimage.s3-website-us-east-1.amazonaws.com')
                
    
class AsarProcessor:
    db = None
    archive_dir = None
    s3_loader = None
    
    def __init__(self, archive_dir = settings.ARCHIVE_DIR, debug=False):
        self.db = GeoDatabase()
        self.db.connect ()
        self.archive_dir  = archive_dir
        self.esa = ESARollingArchive(debug=debug)
        self.satimage_table = 'satimage'
        self.aoi_table = 'satimage_aoi'
        self.s3Loader = S3Loader()
    
    def scrape (self):
        
        records = self.esa.scrape ()
        new_images = 0
        
        logging.info ('Scraped %s records from Envisat Rolling Archive' % (len(records)))
        
        table = self.satimage_table
        sql = "select * from satimage where geo_extent is not null order by acquisition_date desc limit 3"
        cur = self.db.cursor()    
        cur.execute (sql)
        reference_images = cur.fetchall ()

        logging.info ('Checking for new records to add...')

        for record in records:
            # check to see if this image already exists
            if not self.db.item_exists(table, {'name': record['name']}):
                # if not, then add it to the database
                record['description'] = 'Envisat ASAR Radar satellite image acquired %s' % record['acquisition_date']
                record['status'] = 'NEW'
                
                # compute the offset in orbital degrees from the last 3 known orbital locations
                # and then derive an estiamted orbit positiion for the new image based on the time difference
                abs_orbit_positions = []
                for r in reference_images:
                    time_delta = (record['acquisition_date'] + (record['duration']/2)) - (r['acquisition_date'] + (r['duration'] / 2))
                    orbit_delta = (((time_delta.total_seconds() / 60) / 100.6))
                    abs_orbit_positions.append ((r['orbit'] + (r['orbit_position']/360) + orbit_delta))

                print abs_orbit_positions
                s = sum(abs_orbit_positions)
                l = len(abs_orbit_positions)
                value = s/l * 360 % 360
                record ['orbit_position'] = value
                
                # downgrade priority on images 
                if (70 < record ['orbit_position'] < 110) or (240 < record ['orbit_position'] < 300):
                    record['priority'] = 10
                else:
                    record['priority'] = 100
                                    
                id = self.db.insert_item (table, record)                                
                new_images += 1

        logging.info ('Added %s new images.' % (new_images))


    def _get_images (self, status = 'DOWNLOADED', limit = settings.MAX_PROCESS, name = None):
        # get list of images to be processed
        if name:
            sql = "select * from %s where name = '%s'" % (self.satimage_table, name)
        else:
            sql = "select * from %s where status = '%s' order by priority desc, acquisition_date asc limit %s" % (self.satimage_table, status, limit)
        cur = self.db.cursor()    
        cur.execute (sql)
        return cur.fetchall ()

    def _next (self, status = 'DOWNLOADED', new_status = 'PROCESSING', name = None):
        # get next image to be processed
        if name:
            sql = "select * from %s where name='%s' order by acquisition_date asc limit 1" % (self.satimage_table, name)
        else:
            sql = "select * from %s where status = '%s' order by priority desc, acquisition_date asc limit 1" % (self.satimage_table, status)
        cur = self.db.cursor()    
        cur.execute (sql)
        next = cur.fetchone ()
        if next:
            sql = "update %s set status = '%s' where id = %s" % (self.satimage_table, new_status, next['id'])
            cur.execute (sql)
        return next    
    
    def _archive_dir (self, image_name):
        path = os.path.join(self.archive_dir, image_name[14:18], image_name[18:20], image_name[20:22], image_name)
        
        if not os.path.exists(path):
            os.makedirs(path)
            
        return path        
    
    def _s3_path (self, image_name):
        return 's3://satimage/ASAR/%s/%s/%s/%s/' % (image_name[14:18], image_name[18:20], image_name[20:22], image_name)

    
    def _update_image_status (self, status, image_id):
        sql = "update %s set status = '%s' where id = '%s'" % (self.satimage_table, status, image_id)
        cur = self.db.cursor()    
        cur.execute (sql)
    
    def download (self, name = None):
    
        logging.info ("Downloading new images...")
        
        item = self._next (status='NEW', new_status='DOWNLOADING', name=name)
        while item:

            dest_file = os.path.join(self._archive_dir(item['name']), item['name'])
#            print dest_file                         
            result = self.esa.download_image (item['url'], dest_file)
            if result:
                status = 'DOWNLOADED'
            else:
                status = 'ERR_DOWNLOAD'
            
            self._update_image_status (status, item['id'])
            
            if not name:
                item = self._next (status='NEW', new_status='DOWNLOADING')
            else:
                item = None

    def find_intersections (self, name=None, aoi=None):

        sql = """select i.name as image_name, a.id as aoi_id, a.name as aoi_name, i.status as status 
            from %(satimage_table)s i join %(aoi_table)s a 
            on ST_Intersects(i.geo_extent, a.the_geom)
            and (a.begin_date is null or i.acquisition_date >= a.begin_date)
            and (a.end_date is null or i.acquisition_date <= a.end_date)
            """ % {'satimage_table' : self.satimage_table, 'aoi_table': self.aoi_table}
        
        where = []
        if name: where.append (('i.name', name)) 
        if aoi: where.append (('a.name', aoi))
        if where:
            sql += " WHERE " 
            sql += ' AND '.join(["%s = '%s'" % w for w in where])
        cur = self.db.cursor()    
        cur.execute (sql)
        items = cur.fetchall ()

        logging.info ('Found %s intersections for image [%s] and AOI [%s]'%(len(items), name or 'any', aoi or 'any'))
        
        return items

    def aoi_footprint (self, aoi):
        sql = "select ST_Ymax(the_geom) as bbox_lat1, ST_Xmin(the_geom) as bbox_lng1, ST_Ymin(the_geom) as bbox_lat2,  ST_Xmax(the_geom) as bbox_lng2 from %s where name = '%s'" % (self.aoi_table, aoi)
        cur = self.db.cursor()    
        cur.execute (sql)
        item = cur.fetchone ()
        return item


    def process_aoi(self, aoi):
        images = self.find_intersections(aoi=aoi)
        
        if images:
            logging.info ('Processing %s images that intersect with AOI [%s]' % (len(images), aoi)) 
            for image in images:
                logging.info (image) 
                self.process(name=image['image_name'], aoi=aoi)
        else:
            logging.error ('Found no images that intersect with AOI [%s]' % (aoi)) 
            
    def _publish_image (self, source_image, name, filename, image_type, corners):
        s3_path = self._s3_path (source_image)
        url = self.s3Loader.put_file (filename, '%s%s' % (s3_path, os.path.basename(filename) ))   
        poly = ['%(lng)s %(lat)s'%c for c in corners]
        poly.append (poly[0])
        poly_sql = ', '. join(poly)
        poly_sql = "ST_GeomFromText('SRID=4326;POLYGON((%s))')" % poly_sql
        
        values_sql = "%s,%s,%s,%s," + poly_sql        
        sql = "insert into satimage_published (source_image, type, url, name, geo_extent) values (%s)" % values_sql
    
        self.db.exec_sql("delete from satimage_published where name= %s", [name] )    
        self.db.exec_sql(sql, [source_image, image_type, url, name])
        
        
    
    def process(self, name=None, aoi=None, auto_cleanup=True):
        
        # get list of images to be processed
#        items = self._get_images(status='DOWNLOADED', limit=settings.MAX_PROCESS, name=name)
#        logging.info ('Found %s images to process'%len(items))

        cur = self.db.cursor()    
        
        item = self._next (status='DOWNLOADED', new_status='PROCESSING', name=name)
        while item:
        
            logging.info ('Processing %s'%item['name'])

            archive_dir=self._archive_dir(item['name'])
            
            n1_file = os.path.join(archive_dir, item['name'])

            image = AsarImageFile(EnvisatBest(auto_cleanup), n1_file)
            
            status = 'PROCESSED'
            try:
                # extract header
                image.extract_header ()
                
                # extract footprint and store it in the database
                footprint = image.extract_footprint ()
                
                logging.debug (footprint)
                
                if footprint:
                    poly = ['%(lng)s %(lat)s'%c for c in footprint['corners']]
                    poly.append (poly[0])
                    poly_sql = ', '. join(poly)
                    
                    sql = """
update %(table)s 
set pass = '%(pass)s', geo_extent = ST_GeomFromText('SRID=4326;POLYGON((%(poly)s))') 
WHERE id = %(id)s;

update %(table)s set orbit_position = 
case 
	when ST_Y(ST_Centroid(geo_extent))  >= 0 AND pass = 'ASCENDING' THEN 
		ST_Y(ST_Centroid(geo_extent))
	when ST_Y(ST_Centroid(geo_extent))  >= 0 AND pass = 'DESCENDING' THEN 
		180 - ST_Y(ST_Centroid(geo_extent))
	when ST_Y(ST_Centroid(geo_extent))  < 0 AND pass = 'ASCENDING' THEN 
		360 + ST_Y(ST_Centroid(geo_extent))
	when ST_Y(ST_Centroid(geo_extent))  < 0 AND pass = 'DESCENDING' THEN 
		180 - ST_Y(ST_Centroid(geo_extent))
	else
		NULL
end 
WHERE id = %(id)s;
""" % {'table': self.satimage_table, 'pass':footprint['pass'], 'poly': poly_sql, 'id': item['id']}

        #            print sql
                    cur.execute (sql)
                    
                # generate quicklook
                image_name = item['name'].replace ('.N1', '-preview')
                preview_file = os.path.join(os.path.join(archive_dir, '%s.tif' % image_name))
                image.extract_quicklook(preview_file)
                self._publish_image (item['name'], image_name, preview_file, 'PREVIEW', footprint['corners'])
                
                
                # Check for intersections with AOIs
                aois = self.find_intersections (name=item['name'], aoi=aoi)
                if aois:
                    for aoi in aois:
                        bbox = self.aoi_footprint(aoi['aoi_name'])
                        image.extract_fullres (bbox)
                        image.geocorrect ()
                        image.adjust_gain ()
                        image_name = item['name'].replace ('.N1', '-%s' % aoi['aoi_name'])
                        dest_file = os.path.join(os.path.join(archive_dir, '%s.tif' % image_name))
                        image.extract_geotiff (dest_file)
                        corners = image.extract_geotiff_footprint ()
                        
                        self._publish_image (item['name'], image_name, dest_file, 'GEOTIFF', corners)
                
            except Error as e:
                logging.error (e)
                status='ERR_PROCESSING'
                
            self._update_image_status (status, item['id'])
            if not name:
                item = self._next (status='DOWNLOADED', new_status='PROCESSING')
            else:
                item = None
            
    def intersect(self, name=None, aoi=None):
        items = self.find_intersections (name, aoi)
        print "\n"
        for i in items:
            print "%(aoi_name)s: %(image_name)s %(status)s\n" % i
    


# SQL to insert an AOI - REMEMBER!!!! Longitude comes first!!!
# insert into satimage_aoi (name, the_geom) VALUES ('Taylor_DWH', ST_GeomFromText('SRID=4326;POLYGON((-89.25 28.25, -89.25 29.25, -88 29.25, -88 28.25, -89.25 28.25))'))
    def test (self, options):
        
        table = self.satimage_table
        sql = "select * from satimage where geo_extent is not null order by acquisition_date desc limit 3"
        cur = self.db.cursor()    
        cur.execute (sql)
        reference_images = cur.fetchall ()

        for r in reference_images:
            print "%(acquisition_date)s\n%(duration)s\n%(orbit_position)s\n\n" % r    
        print "\n"
        
        sql = "select * from satimage where acquisition_date < '2012-01-14 04:42:00'  order by acquisition_date desc limit 10"
        cur = self.db.cursor()    
        cur.execute (sql)
        records = cur.fetchall ()
        
        for record in records:
            # check to see if this image already exists
#            if not self.db.item_exists(table, {'name': record['name']}):
                # if not, then add it to the database

            record['description'] = 'Envisat ASAR Radar satellite image acquired %s' % record['acquisition_date']
            record['status'] = 'NEW'
            
            # compute the offset in orbital degrees from the last 3 known orbital locations
            # and then derive an estiamted orbit positiion for the new image based on the time difference
            sum = 0
            for r in reference_images:
                time_delta = (record['acquisition_date'] + (record['duration']/2)) - (r['acquisition_date'] + (r['duration'] / 2))
                orbit_delta = ((time_delta.total_seconds() / 60 % 100.6) * 360 / 100.6)
                orbit_position = (r['orbit_position'] + orbit_delta) % 360
                
                print "%s %s %s" % (time_delta, orbit_delta, orbit_position)
                
                # offset by 180 degrees so we don't end up averaging across the 0 , 360 degree discontinuity
                sum += orbit_position - 180
            orbit_position = (sum / len(reference_images)) + 180    

            record ['new_orbit_position'] = orbit_position
            print "%(acquisition_date)s\n%(duration)s\n%(orbit_position)s\n%(new_orbit_position)s\n" % record  
            
            # downgrade priority on images 
            if (orbit_position > 70 and orbit_position < 110) or (orbit_position > 240 and orbit_position < 300):
                record['priority'] = 10
            else:
                record['priority'] = 100
                                
#            id = self.db.insert_item (table, record)                                
#            new_images += 1
                    
            print "" % record
                
        
#def test_footprint ():
#    best = EnvisatBest ()
#    
#    n1_file = 'Z:\\Skytruth\\util\\asar\\data\\test\\ASA_WSM_1PNPDE20111220_142144_000000923109_00413_51289_1383.N1'
#
#    image = AsarImageFile(best, n1_file)
#    
#    image.extract_header ()
#
#    footprint = image.extract_footprint ()
#
#    print footprint
#
#
#def test_scrape ():
#    esa = ESARollingArchive()
#    
#    records = esa.scrape ()
#    
#    print records
#
#def test_download ():
##    url= 'https://oa-es.eo.esa.int/ASA/ASA_IMM_1PNPDE20111211_070800_000000503109_00279_51155_8290.N1'
#    url= 'https://oa-es.eo.esa.int/ASA/ASA_WSM_1PNPDE20111220_142144_000000923109_00413_51289_1383.N1'
#
#    esa = ESARollingArchive()
#    esa.dowload_image (url, 'data/test/ASA_WSM_1PNPDE20111220_142144_000000923109_00413_51289_1383.N1')
#        
#def test_extract ():
##    n1_file = 'data/test/ASA_IMM_1PNPDE20111211_070800_000000503109_00279_51155_8290.N1'
##    bbox = [[37,44], [38,45]]
##    n1_file = 'data/test/ASA_WSM_1PNPDE20111220_142144_000000923109_00413_51289_1383.N1'
##    bbox = [[8,117], [9,118]]
#    n1_file = 'data/test/ASA_APM_1PNPDE20111220_160551_000001153109_00414_51290_1385.N1'
#    bbox = [[24,88], [25,89]]
#
#
#
#    tif_file = '%s.clipped.tif' % n1_file  
#    
#    best = EnvisatBest ()
#    image = AsarImageFile(best, n1_file)
#    image.extract_header () 
#    print image.extract_footprint ()
#    image.extract_fullres (bbox)
#    image.geocorrect ()
#    image.adjust_gain()
#    image.extract_geotiff (tif_file)
#    
    
def main ():

    desc = "Tools for acquiring and processing ASAR satellite image files"
    
    usage = """%prog [options] command

    command is one of the following:
        scrape      
            scrape the rolling archive and add new images to the processing queue
        download    
            download pending images in the queue
            use --name to process a specifc image
        process     
            process dowloaded images - capture footprint and generate preview
            use --name to process a specifc image
        intersect     
            Find intersections between processed images and AOIs
            use --name to process a specifc image
"""
    parser = OptionParser(description=desc, usage=usage)

    parser.set_defaults(loglevel=logging.INFO)
    parser.add_option("-q", "--quiet",
                          action="store_const", dest="loglevel", const=logging.ERROR, 
                          help="Only output error messages")
    parser.add_option("-v", "--verbose",
                          action="store_const", dest="loglevel", const=logging.DEBUG, 
                          help="Output debugging information")
    parser.add_option("-n", "--name",
                          dest="name",metavar='NAME',
                          help="Specify the name of a particular image to operate on")
    parser.add_option("-a", "--aoi",
                          dest="aoi",metavar='AOI',
                          help="Specify the name of a particular AOI to operate on")
    parser.add_option("-c", "--nocleanup",
                          dest="autocleanup", action="store_false",default=True,
                          help="Suppress cleanup of temporary files on exit")

                          
    (options, args) = parser.parse_args()
    
    if len(args) < 1:
        parser.error("Not enough arguments")
        
    command = args[0].lower()
        
    logging.basicConfig(format='%(levelname)s: %(message)s', level=options.loglevel)

    processor = AsarProcessor(debug= options.loglevel==logging.DEBUG)
    if command == 'scrape':
        processor.scrape ()
    elif command == 'download':
        processor.download (name = options.name)
    elif command == 'process':
        if options.aoi:
            processor.process_aoi (aoi=options.aoi)
        else:
            processor.process (name = options.name, auto_cleanup = options.autocleanup)
    elif command == 'intersect':
        processor.intersect (name = options.name, aoi = options.aoi)
    elif command == 'test':
        processor.test (options)
    else:
        parser.error ('Unknown command: %s' % command)
    
    del processor
    logging.info ("Done.")
         
if __name__ == "__main__":
    main ()