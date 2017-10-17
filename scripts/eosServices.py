import os, sys, re, types, MySQLdb
from string import Template
from datetime import datetime, timedelta
from SOAPpy import WSDL

import sciflo
from eosUtils.db import CONFIGURED_DATASETS, queryDataset

#this dir
THIS_DIR=os.path.dirname(os.path.abspath(__file__))

#xml dir; handle exposing via python web server or apache cgi
XML_DIR=os.path.join(sys.prefix, 'etc', 'crawler')
if not os.path.isdir(XML_DIR): raise RuntimeError, \
    "Unable to find xml configuration dir %s." % XML_DIR

#dataset to getDataById mapping
DATASET_TO_FUNC_MAP={
    'GPS': 'getGpsDataById',
    'CloudSat': 'getCloudSatDataById',
    'AIRS': 'getAirsDataById',
    'UCAR-GPS': 'getUcarGpsDataById',
    'inst3_3d_asm_Cp': 'getMerraDataById',
    'tavg3_3d_tdt_Cp': 'getMerraDataById',
    'tavg3_3d_qdt_Cp': 'getMerraDataById',
    'tavg1_2d_flx_Nx': 'getMerraDataById',
    'tavg1_2d_rad_Nx': 'getMerraDataById',
    'tavg1_2d_slv_Nx': 'getMerraDataById',
    'ALOS': 'getAlosDataById',
}

OBJECTIDREGEX_TO_DATASET_MAP={
    '^\d{8}_\d{4}\w{3}_g\d{2}_.*?': 'GPS',
    '^\d{13}_\d{5}_CS.*?': 'CloudSat',
    '^AIRS': 'AIRS',
    '^wetPrf_': 'UCAR-GPS',
    '^ecmPrf_': 'UCAR-GPS',
    '^ecmPrf_': 'UCAR-GPS',
    '^MERRA': 'MERRA',
    '^ALPSRP\d{9}': 'ALOS',
}

#MERRA datasets
MERRA_DATASETS = (
    'inst3_3d_asm_Cp',
    'tavg3_3d_tdt_Cp',
    'tavg3_3d_qdt_Cp',
    'tavg1_2d_flx_Nx',
    'tavg1_2d_rad_Nx',
    'tavg1_2d_slv_Nx',
)

#select by time, space, and version
SELECT_TMPL = Template('''SELECT objectid, starttime, endtime, longitude, latitude, AsText(georing) from $table \
where ((starttime between "$starttime" and "$endtime") or \
(endtime between "$starttime" and "$endtime")) and \
$version \
MBRIntersects(georing, PolygonFromText('POLYGON (($lonmin $latmin,$lonmin $latmax,\
$lonmax $latmax,$lonmax $latmin,$lonmin $latmin))', 4326));''')

#select by time
TIME_SELECT_TMPL = Template('''SELECT objectid, starttime from $table \
where ((starttime between "$starttime" and "$endtime"));''')

#ucar cosmic database schema
ucarCosmicSchema = 'mysql://sflops@127.0.0.1:3306/cosmic'
ucarCosmicUrlSchema = 'mysql://sflops@127.0.0.1:3306/urlCatalog'

def getGeoRegionInfo(datasetName, level, version, startDateTime, endDateTime, latMin,
                     latMax, lonMin, lonMax, responseGroups='Medium'):

    if datasetName in DATASET_TO_FUNC_MAP.keys():

        #Handle GPS
        if datasetName=='GPS':
            db = MySQLdb.connect(db='geoInfo', host='127.0.0.1', port=3306, user='sflops')
            c = db.cursor()

            #get version
            if version == '*' or version == 'None' or version is None: version = ''
            if version != '':
                if re.search(r',', version):
                    sqlversion = '(' + ' OR '.join(['version LIKE "%s"' % i for i in version.split(',')]) + ') and '
                else: sqlversion = 'version = "%s" and ' % version
            selectSt = SELECT_TMPL.substitute(table='gps', starttime=startDateTime, endtime=endDateTime,
                                              version=sqlversion, latmin=latMin, latmax=latMax, 
                                              lonmin=lonMin, lonmax=lonMax)
            c.execute(selectSt) 
            objectids = []
            infoLoL = []
            headerLoL = ['objectid', 'starttime', 'endtime', 'lat', 'lon']
            for objectid, starttime, endtime, longitude, latitude, wkt in c.fetchall():
                objectids.append(objectid)
                infoLoL.append([objectid, starttime.isoformat(), endtime.isoformat(), latitude, longitude])
                #xmin, ymin, xmax, ymax = Geometry.fromWKT(wkt, srs=srs).envelope().totuple()
                #print objectid, starttime, endtime, longitude, latitude, xmin, ymin, xmax, ymax

        #Handle AIRS, MODIS-Terra/Aqua, CloudSat, and MERRA
        else:
            returnTimeSpaceInfoFlag=False
            if responseGroups in ('Large','Medium', 'Small'): returnTimeSpaceInfoFlag=True

            # handle MERRA
            if datasetName in MERRA_DATASETS:
                thisDt = sciflo.utils.getDatetimeFromString(startDateTime)
                thisDt = datetime(thisDt.year, thisDt.month, thisDt.day)
                endDt = sciflo.utils.getDatetimeFromString(endDateTime)
                endDt = datetime(endDt.year, endDt.month, endDt.day)
                infoDict = {}
                while thisDt <= endDt:
                    objectid = 'MERRA300.prod.assim.%s.%04d%02d%02d' % \
                        (datasetName, thisDt.year, thisDt.month, thisDt.day) 
                    starttime = "%04d-%02d-%02dT00:00:00Z" % (thisDt.year, thisDt.month, thisDt.day) 
                    endtime = "%04d-%02d-%02dT23:59:59Z" % (thisDt.year, thisDt.month, thisDt.day) 
                    infoDict[objectid] = {
                        'starttime': starttime,
                        'endtime': endtime,
                        'lonMin': -180.,
                        'lonMax': 180.,
                        'latMin': -90.,
                        'latMax': 90.
                    }
                    thisDt += timedelta(seconds=86400)
            # handle everything else
            else:
                tableId = datasetName
                infoDict=queryDataset(tableId, startDateTime, endDateTime, latMin,
                    latMax, lonMin, lonMax, returnTimeSpaceInfo=returnTimeSpaceInfoFlag)

            if isinstance(infoDict,(types.ListType,types.TupleType)): objectids=infoDict
            else: objectids=infoDict.keys()
            objectids.sort()
            if responseGroups in ('Large','Medium','Small'):
                if datasetName == 'ALOS':
                    infoLoL=[[i,infoDict[i]['starttime'],infoDict[i]['endtime'],
                              infoDict[i]['lonMin'],infoDict[i]['lonMax'],infoDict[i]['latMin'],
                              infoDict[i]['latMax'],infoDict[i]['frameNumber'],infoDict[i]['orbitNumber'],
                              infoDict[i]['orbitRepeat'],infoDict[i]['trackNumber']] for i in objectids]
                    headerLoL=['objectid','starttime','endtime','lonMin','lonMax','latMin','latMax',
                               'frameNumber','orbitNumber','orbitRepeat','trackNumber']
                else:
                    infoLoL=[[i,infoDict[i]['starttime'],infoDict[i]['endtime'],
                              infoDict[i]['lonMin'],infoDict[i]['lonMax'],infoDict[i]['latMin'],
                              infoDict[i]['latMax']] for i in objectids]
                    headerLoL=['objectid','starttime','endtime','lonMin','lonMax','latMin','latMax']

    else: raise RuntimeError("Cannot recognize datasetName %s." % datasetName)

    #generate results 'Objectid' or 'Small' response groups
    if responseGroups == 'Objectid':
        return (responseGroups, objectids, 'objectid')
    if responseGroups == 'Small':
        return (responseGroups, infoLoL, headerLoL)

    #generate results for the rest of the response groups
    getDataByUrlFunc = eval(DATASET_TO_FUNC_MAP[datasetName])
    urlDict = getDataByUrlFunc(objectids, level=level, version=version)
    if responseGroups == 'Url':
        infoLoL = []
        headerLoL = ['objectid', 'url']
        for objectid in objectids:
            try: url = urlDict.get(objectid, [])[0]
            except IndexError: url = None
            infoLoL.append([objectid, url])
        return (responseGroups, infoLoL, headerLoL)
    elif responseGroups == 'Urls':
        infoLoL = []
        headerLoL = ['objectid', ['urls', 'url']]
        for objectid in objectids:
            urls = urlDict.get(objectid, [])
            infoLoL.append([objectid, urls])
        return (responseGroups, infoLoL, headerLoL)
    elif responseGroups == 'Medium':
        headerLoL.append('url')
        for idx in range(len(infoLoL)):
            objectid = infoLoL[idx][0]
            try: url = urlDict.get(objectid, [])[0]
            except IndexError: url = None
            infoLoL[idx].append(url)
        return (responseGroups, infoLoL, headerLoL)
    elif responseGroups == 'Large':
        headerLoL.append(['urls', 'url'])
        for idx in range(len(infoLoL)):
            objectid = infoLoL[idx][0]
            urls = urlDict.get(objectid, [])
            infoLoL[idx].append(urls)
        return (responseGroups, infoLoL, headerLoL)
    else:
        raise RuntimeError("Cannot recognize responseGroups %s." % responseGroups)

def geoRegionQuery(datasetName, level, version, startDateTime, endDateTime, latMin,
                   latMax, lonMin, lonMax, responseGroups='Medium'):
    """Return geo region info results as xml."""

    #handle UCAR-GPS
    if datasetName == 'UCAR-GPS':
        return queryUcarGps(datasetName, level, version, startDateTime, endDateTime, latMin,
                            latMax, lonMin, lonMax, responseGroups)

    (responseGroups, infoLoL, headerLoL) = getGeoRegionInfo(datasetName, level, version,
        startDateTime, endDateTime, latMin, latMax, lonMin, lonMax, responseGroups)
    if responseGroups == 'Objectid':
        return sciflo.utils.simpleList2Xml(infoLoL, 'resultSet', headerLoL, {'id':datasetName})
    if responseGroups in ('Url', 'Urls', 'Small', 'Medium', 'Large'):
        return sciflo.utils.list2Xml(infoLoL, headerLoL, 'resultSet', 'result',{'id':datasetName})
    raise RuntimeError("Cannot recognize responseGroups %s." % responseGroups)

def sortUrlList(urlList):
    """Return ordered url list (localFile, DAP, HTTP, FTP)."""
    #localList = [url for url in urlList if os.path.exists(url)]
    #dodsList = [url for url in urlList if sciflo.utils.isDODS(url)]
    #httpList = [url for url in urlList if not sciflo.utils.isDODS(url) and url.startswith('http')]
    #ftpList = [url for url in urlList if url.startswith('ftp')]
    #localList.extend(dodsList); localList.extend(httpList); localList.extend(ftpList)
    fileUrlList = []
    localList = []
    dodsList = []
    httpList = []
    ftpList = []
    allList = []
    for url in urlList:
        if isinstance(url, types.StringTypes) and '.xfr' in url: continue
        if os.path.exists(url): localList.insert(0,url)
        elif url.startswith('file://'): fileUrlList.insert(0, url)
        elif url.startswith('http') and re.search(r'(dods|opendap)',url,re.IGNORECASE): dodsList.insert(0,url)
        elif url.startswith('http'):
            if '.ecs.nasa.gov' in url: httpList.insert(0,url)
            else: httpList.append(url)
        else: ftpList.append(url)
    localList.sort(); localList.reverse()
    #allList.extend(dodsList); allList.extend(ftpList); allList.extend(httpList)
    #allList.extend(localList); allList.extend(fileUrlList)
    allList.extend(ftpList); allList.extend(httpList); allList.extend(dodsList)
    allList.extend(localList); allList.extend(fileUrlList)
    return allList

def getDataById(xmlConfigFile, objectids, mungeMatch=None, reMode=False, level=None, version=None):
    """Create a Librarian and catalog object to query a catalog
    by objectid.  Return xml result set of objectDataUrl's.
    Optionally, you can pass in a tuple of strings (<to match>,<replace with>)
    to munge any urls.
    """

    #objectid dict
    objectidDict = {}

    #get the librarian object
    libobj = sciflo.catalog.ScifloLibrarian(xmlConfigFile)
    instr = libobj.getInstrument()

    #create the catalog object
    container = 'mysql://sflops@127.0.0.1:3306/urlCatalog'
    catalogObj = sciflo.catalog.SqlAlchemyCatalog(container)

    #set catalog object for librarian
    libobj.setCatalog(catalogObj)

    #sort objectids
    objectids.sort()

    #are we munging anything
    if mungeMatch:
        for objectid in objectids:
            results = catalogObj.query(objectid)
            urlList = []
            if results:
                for result in list(set(results)):
                    for matchStr, repStr in mungeMatch:
                        if matchStr is None and repStr is None:
                            if result not in urlList: urlList.append(result)
                        else:
                            if isinstance(repStr, types.StringType): repStr = [repStr]
                            for repStrItem in repStr:
                                if reMode and re.search(matchStr, result): urlList.append(re.sub(matchStr,repStrItem,result))
                                elif result.startswith(matchStr): urlList.append(result.replace(matchStr,repStrItem))
            #filter by level and version
            if level not in [None, 'None']:
                urlList = [i for i in urlList if re.search(level, i)]
            if version not in [None, 'None']:
                urlList = [i for i in urlList if re.search(version, i)]
            objectidDict[objectid] = sortUrlList(urlList)
    else:
        for objectid in objectids:
            results = catalogObj.query(objectid)
            if results:
                urlList = list(set(results))
                #filter by level and version
                if level not in [None, 'None']:
                    urlList = [i for i in urlList if re.search(level, i)]
                if version not in [None, 'None']:
                    urlList = [i for i in urlList if re.search(version, i)]
                objectidDict[objectid]=sortUrlList(urlList)
    return objectidDict

def getGpsDataById(objectids, level=None, version=None):
    """Return a list of urls for each objectid."""

    #crawler xml config file
    xmlConfigFile = os.path.join(XML_DIR, 'GPSL2Crawler.xml')

    #return
    return getDataById(xmlConfigFile, objectids, [#(None, None),
        ('/home/sflops/sciflo/share/sciflo/data/sensors/genesis/glevels',
         ['http://${CVO}/sciflo/data/sensors/genesis',
         'http://${CVO}:8080/opendap/sensors/genesis'])],
        reMode=True, level=level, version=version)

def getUcarGpsDataById(objectids, level=None, version=None):
    """Return a list of urls for each objectid."""

    #crawler xml config file
    xmlConfigFile = os.path.join(XML_DIR, 'UCARL2Crawler.xml')

    #return
    return getDataById(xmlConfigFile, objectids, [#(None, None),
        ('/home/sflops/sciflo/share/sciflo/data',
         ['http://${CVO}/sciflo/data', 'http://${CVO}:8080/opendap'])],
        reMode=True, level=level, version=version)

def getCloudSatDataById(objectids, level=None, version=None):
    """Return a list of urls for each objectid."""

    #crawler xml config file
    xmlConfigFile = os.path.join(XML_DIR, 'CloudSatCrawler.xml')

    #return
    return getDataById(xmlConfigFile, objectids, [#(None,None),
        ('/home/sflops/sciflo/share/sciflo/data',
         ['http://${CVO}/sciflo/data', 'http://${CVO}:8080/opendap']),
        ('ftp://ftp1.cloudsat.cira.colostate.edu/users/${USER}', ['ftp://ftp1.cloudsat.cira.colostate.edu']),
        ], reMode=True, level=level, version=version)

def getAirsDataById(objectids, level=None, version=None):
    """Return a list of urls for each objectid."""

    #crawler xml config file
    xmlConfigFile=os.path.join(XML_DIR,'AIRSL2RetStdCrawler.xml')

    #return(munge into http url)
    return getDataById(xmlConfigFile,objectids,[(None,None),
        ('file://${WEATHER}/tds/archive', ['file://${WEATHER}/archive']),
        ('/home/sflops/sciflo/share/sciflo/data',
         ['http://${CVO}/sciflo/data', 'http://${CVO}:8080/opendap']),
        ('ftp://airspar1u.ecs.nasa.gov/data/s4pa', ['http://airspar1u.ecs.nasa.gov/opendap']),
        ('ftp://airscal1u.ecs.nasa.gov/data/s4pa', ['http://airscal1u.ecs.nasa.gov/opendap']),
        ('ftp://airscal2u.ecs.nasa.gov/data/s4pa', ['http://airscal2u.ecs.nasa.gov/opendap']),
        ], reMode=True, level=level, version=version)

def getMerraDataById(objectids, level=None, version=None):
    """Return a list of urls for each objectid."""

    #crawler xml config file
    xmlConfigFile = os.path.join(XML_DIR, 'MERRA.xml')

    #return
    return getDataById(xmlConfigFile, objectids, [(None,None),
        ('ftp://goldsmr3.sci.gsfc.nasa.gov/data/s4pa', 
         ['http://goldsmr3.sci.gsfc.nasa.gov/data/s4pa','http://goldsmr3.sci.gsfc.nasa.gov/opendap']),
        ('ftp://goldsmr2.sci.gsfc.nasa.gov/data/s4pa', 
         ['http://goldsmr2.sci.gsfc.nasa.gov/data/s4pa','http://goldsmr2.sci.gsfc.nasa.gov/opendap']),
        ], reMode=True, level=level, version=version)

def getAlosDataById(objectids, level=None, version=None):
    """Return a list of urls for each objectid."""

    #crawler xml config file
    xmlConfigFile=os.path.join(XML_DIR,'ALOSCrawler.xml')

    #return(munge into http url)
    return getDataById(xmlConfigFile,objectids,[(None,None)],
        reMode=True, level=level, version=version)

def findDataById(objectids, level=None, version=None):
    """Return xml list of urls for each objectid."""

    if sciflo.utils.isXml(objectids):
        et, xmlNs = sciflo.utils.getXmlEtree(objectids)
        objectids = et.xpath('.//_default:objectid/text()', xmlNs)
    infoLoL = []
    headerLoL = ['objectid', ['urls', 'url']]
    if len(objectids) == 0: return sciflo.utils.list2Xml(infoLoL, headerLoL, 'resultSet', 'result')
    datasetDict = {}
    for regex in OBJECTIDREGEX_TO_DATASET_MAP.keys():
        datasetDict[OBJECTIDREGEX_TO_DATASET_MAP[regex]] = []
    for objectid in objectids:
        found = False
        for regex in OBJECTIDREGEX_TO_DATASET_MAP.keys():
            if re.search(regex, objectid):
                datasetDict[OBJECTIDREGEX_TO_DATASET_MAP[regex]].append(objectid)
                found = True
                break
        if not found:
            raise RuntimeError("Failed to match objectid %s to a dataset." % objectid)
    datasetsToDo = [dataset for dataset in datasetDict.keys() if len(datasetDict[dataset]) > 0]
    if len(datasetsToDo) > 1:
        raise NotImplementedError("Multiple dataset handling not yet implemented.")
    getDataByUrlFunc = eval(DATASET_TO_FUNC_MAP[datasetsToDo[0]])
    urlDict = getDataByUrlFunc(datasetDict[datasetsToDo[0]], level=level, version=version)
    objids = datasetDict[datasetsToDo[0]]
    for objid in objectids:
        urls = urlDict.get(objid, [])
        infoLoL.append([objid, urls])
    return sciflo.utils.list2Xml(infoLoL, headerLoL, 'resultSet', 'result')

CATALOG_SCHEMA_XML = '''<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="catalogEntry">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="objectid"/>
                <xs:element ref="objectDataSet"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="objectid" type="xs:string"/>
    <xs:element name="objectDataSet">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="objectData" type="xs:string"
                minOccurs="1" maxOccurs="999"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
'''

LOCAL_RE = re.compile(r'^/home/sflops/sciflo/share/sciflo/(.*)$')

def queryUcarGps(datasetName, level, version, startDateTime, endDateTime, latMin,
                   latMax, lonMin, lonMax, responseGroups):
    from ucarCosmic import UcarMetadata, UcarMetadataHandler
    from sqlalchemy.orm import clear_mappers
    from sciflo.catalog import SqlAlchemyCatalog, Urls
    import cPickle
    sac = SqlAlchemyCatalog(ucarCosmicUrlSchema)
    umh = UcarMetadataHandler(ucarCosmicSchema)
    recs = umh.session.query(UcarMetadata).filter( \
        UcarMetadata.lat.between(latMin, latMax) & \
        UcarMetadata.lon.between(lonMin, lonMax) & ( \
        UcarMetadata.start_time.between(startDateTime, endDateTime) | \
        UcarMetadata.stop_time.between(startDateTime, endDateTime)))
    if level not in (None, 'None'): recs = recs.filter(UcarMetadata.objectid.like('%%%s%%' % level))
    if version not in (None, 'None'): recs = recs.filter(UcarMetadata.objectid.like('%%%s%%' % version))
    infoLoL = []
    headerLoL = ['objectid', 'starttime', 'endtime', 'lat', 'lon']
    setHeader = False
    for rec in recs.all():
        if re.search(r'^atmPrf', rec.objectid): continue
        if responseGroups=='Objectid':
            infoLoL.append(rec.objectid)
            if not setHeader:
                headerLoL = 'objectid'
                setHeader = True
        else:
            if responseGroups=='Small':
                infoLoL.append([rec.objectid, rec.start_time, rec.stop_time, rec.lat, rec.lon])
                continue
            urlResults = sac.session.query(Urls).filter_by(objectid=rec.objectid).all()
            urls = []
            if len(urlResults) > 0: urls = cPickle.loads(urlResults[0].url_list)
            for thisUrl in urls:
                localMatch = LOCAL_RE.search(thisUrl)
                if localMatch:
                    urls.append('http://${CVO}/sciflo/%s' % localMatch.group(1))
                    break
            if responseGroups in ('Urls', 'Large'):
                infoLoL.append([rec.objectid, rec.start_time, rec.stop_time, rec.lat, rec.lon, urls])
                if not setHeader:
                    headerLoL.append(['urls', 'url'])
                    setHeader = True
            else:
                infoLoL.append([rec.objectid, rec.start_time, rec.stop_time, rec.lat, rec.lon, urls[0]])
                if not setHeader:
                    headerLoL.append('url')
                    setHeader = True
    
    if responseGroups=='Objectid':
        return sciflo.utils.simpleList2Xml(infoLoL, 'resultSet', headerLoL, {'id':datasetName})
    else:
        return sciflo.utils.list2Xml(infoLoL, headerLoL, 'resultSet', 'result',{'id':datasetName})
    raise RuntimeError, "Cannot recognize responseGroups %s." % responseGroups
