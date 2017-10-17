import os, sys, re, types, MySQLdb, time
from string import Template
from datetime import datetime, timedelta
from SOAPpy import WSDL

import sciflo
from eosUtils.granule2 import getGranuleIdsByOrbitTable, DATASETINFO_TO_TABLE_MAP

OBJECTIDREGEX_TO_DATASET_MAP={
    '^\d{8}_\d{4}\w{3}_g\d{2}_.*?': 'GPS',
    '^\d{13}_\d{5}_CS.*?': 'CloudSat',
    '^AIRS': 'AIRS',
    '^wetPrf_': 'UCAR-GPS',
    '^ecmPrf_': 'UCAR-GPS',
}

def getMatchupIndicesByAirs(objectids):
    """Return xml list of urls to matchup index files for each objectid."""

    infoLoL = []
    headerLoL = ['objectid', ['urls', 'url']]
    if isinstance(objectids, types.StringTypes): raise RuntimeError("No AIRS granules found.")
    if len(objectids) == 0: return sciflo.utils.simpleList2Xml(infoLoL, headerLoL, 'resultSet', 'result')
    for objectid in objectids:
        match = re.search(r'^AIRS\.((\d{4})\.(\d{2})\.(\d{2})\.(\d{3}))$', objectid._name)
        urls = \
            ['http://${MSAS}/measures.fetzer/pickles/airs.aqua_cloudsat/v3.1/%s/%s/%s/index-airs.aqua_cloudsat-v3.1-%s.pkl'
            % (match.group(2), match.group(3), match.group(4), match.group(1))]
        infoLoL.append([objectid._name, urls])
    return sciflo.utils.list2Xml(infoLoL, headerLoL, 'resultSet', 'result')
