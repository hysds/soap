from occult.occultation import GpsGenGrid
from sciflo.utils import (getXmlEtree, ScifloConfigParser, 
simpleList2Xml)
import os, sys, shutil, hashlib

#-----------------------------------------------------------------------------
#soap methods
#-----------------------------------------------------------------------------
ROOT_GLEVELS_DIR = '/data1/ftp/pub/genesis/glevels'

def aggregateOverObjectidList(xmlString):
    """
    Usage: aggregateOverObjectidList(xmlString)
    This function takes an arbitrary xml string and
    extracts the datasetids via the <objectid> tag.
    For each datasetid, it determines whether or not
    a corresponding L2 txt file exists locally and creates
    a CDAT generic grid of all profiles in NetCDF.  It
    returns the xml result set of the url pointing to the
    NetCDF file."""

    #get dir & url config
    scp = ScifloConfigParser()
    webUrl = scp.getParameter('htmlBaseHref')
    dataUrl = webUrl.replace('/web/', '/data/')
    dataDir = os.path.join(sys.prefix, 'share', 'sciflo', 'data')
    gpsGenDir = os.path.join(dataDir, 'gps', 'genGrids')
    gpsGenUrl = os.path.join(dataUrl, 'gps', 'genGrids')
    if not os.path.isdir(gpsGenDir): os.makedirs(gpsGenDir)
    
    #get occids
    e, nsDict = getXmlEtree(xmlString)
    print xmlString
    objectids = [i for i in e.xpath('.//_default:objectid/text()', nsDict)]
    print objectids
    objectids.sort()

    #sort and generate hash
    hash = hashlib.md5(','.join(objectids)).hexdigest() + '.nc'
    ncFile = os.path.join(gpsGenDir, hash)
    ncUrl = os.path.join(gpsGenUrl, hash)

    #create generic grid if it doesn't exist
    if not os.path.isfile(ncFile):

        #create generic grid
        g = GpsGenGrid(objectids, addModel=['ncep','ecmwf'], root=ROOT_GLEVELS_DIR)
        n = g.writeNc(ncFile)

    return simpleList2Xml([ncUrl], 'ResultSet', 'url')

def aggregateOverObjectidListUnaltered(xmlString):
    """
    Usage: aggregateOverObjectidList(xmlString)
    This function takes an arbitrary xml string and
    extracts the datasetids via the <objectid> tag.
    For each datasetid, it determines whether or not
    a corresponding L2 txt file exists locally and creates
    a CDAT generic grid of all profiles in NetCDF.  It
    returns the xml result set of the url pointing to the
    NetCDF file."""

    #get dir & url config
    scp = ScifloConfigParser()
    webUrl = scp.getParameter('htmlBaseHref')
    dataUrl = webUrl.replace('/web/', '/data/')
    dataDir = os.path.join(sys.prefix, 'share', 'sciflo', 'data')
    gpsGenDir = os.path.join(dataDir, 'gps', 'genGrids')
    gpsGenUrl = os.path.join(dataUrl, 'gps', 'genGrids')
    if not os.path.isdir(gpsGenDir): os.makedirs(gpsGenDir)
    
    #get occids
    e, nsDict = getXmlEtree(xmlString)
    print xmlString
    objectids = [i for i in e.xpath('.//_default:objectid/text()', nsDict)]
    print objectids
    #objectids.sort()

    #sort and generate hash
    hash = hashlib.md5(','.join(objectids)).hexdigest() + '.nc'
    ncFile = os.path.join(gpsGenDir, hash)
    ncUrl = os.path.join(gpsGenUrl, hash)

    #create generic grid if it doesn't exist
    if not os.path.isfile(ncFile):

        #create generic grid
        g = GpsGenGrid(objectids, addModel=['ncep','ecmwf'],
                       root=ROOT_GLEVELS_DIR, doSort=False)
        n = g.writeNcUnaltered(ncFile)

    return simpleList2Xml([ncUrl], 'ResultSet', 'url')
