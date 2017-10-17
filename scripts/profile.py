import cProfile
from eosServices import geoRegionQuery

def run():
    geoRegionQuery('GPS', 'L2', None, '2006-03-01 00:00:00', '2007-03-01 00:59:59', -90.0, 90.0, -180.0, 180.0, "Small")

cProfile.run('run()', 'grq')
