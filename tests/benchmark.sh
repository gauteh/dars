#! /bin/bash

set -ep

echo "Make sure you run 'docker-compose up' first."

DARS="http://localhost:8001/data"
TDS="http://localhost:8002/thredds/dodsC/test/data"
WRK="wrk -c100 -t12"

pytest --quiet --benchmark-only

# echo
# echo
# echo "Small dataset (coads_climatology.nc - 3.1mb)"
# echo "Dump through DAP"
# echo

# echo -n "dars: "
# time ncdump "$DARS/coads_climatology.nc" > /dev/null

# echo -n "tds:  "
# time ncdump "$TDS/coads_climatology.nc" > /dev/null


# echo
# echo "Fetch DAS"

# echo -n "dars: "
# $WRK "$DARS/coads_climatology.nc.das"

# echo -n "tds:  "
# $WRK "$TDS/coads_climatology.nc.das"

# echo
# echo "Fetch DDS"

# echo -n "dars: "
# $WRK "$DARS/coads_climatology.nc.dds"

# echo -n "tds:  "
# $WRK "$TDS/coads_climatology.nc.dds"

# echo
# echo "Fetch SST"

# echo -n "dars: "
# $WRK "$DARS/coads_climatology.nc.dods?SST.SST"

# echo -n "tds:  "
# $WRK "$TDS/coads_climatology.nc.dods?SST.SST"

echo
echo
echo "Large dataset (1.5gb)"
echo "Dump through DAP"
echo

echo -n "dars: "
time ncdump "$DARS/meps_det_vc_2_5km_latest.nc" > /dev/null

echo -n "tds:  "
time ncdump "$TDS/meps_det_vc_2_5km_latest.nc" > /dev/null
