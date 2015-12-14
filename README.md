Tools to configure and read data from various sensing devices, then
save the data to database and/or various collection/analysis services.

Copyright: Matthew Wall, all rights reserved

License: GPLv3

Pre-Requisites:
- Python 2.6 or Python 2.7 (Python 3 is not supported)
- python-serial (required for serial connections)
- python-mysqldb (required if saving to mysql database)
- python-sqlite3 (required if saving to sqlite database)
- python-rrdtool (required if saving to round-robin database)


brultech power monitors

btmon.py - read data from ecm-1220, ecm-1240, or green-eye device
btcfg.py - configure the green-eye monitor
btarc.py - archive data in mysql database
ecmread.py - precursor to btmon.py


radio thermostat devices

rtmon.py - read data from ct-30, ct-50, ct-80 devices


embedded data systems one-wire server

edsmon.py - read data from one-wire server


ted5000

tedmon.py - read data from ted5000
