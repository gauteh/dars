#! /usr/bin/env bash

set -ep

dars="http://localhost:8001/data/"
thredds="http://localhost:8002/thredds/dodsC/test/data/"
hyrax="http://localhost:8003/opendap/"

das_1="meps_det_vc_2_5km_latest.nc.das"
dds_1="meps_det_vc_2_5km_latest.nc.dds"
dods_1="meps_det_vc_2_5km_latest.nc.dods"

function uriencode { jq -nr --arg v "$1" '$v|@uri'; }

big_slice="x_wind_ml.x_wind_ml[1:66][0:64][0][0:28386]"
small_slice="x_wind_ml.x_wind_ml[1:10][0][0][0:1000]"

big_slice=$(uriencode "${big_slice}")
small_slice=$(uriencode "${small_slice}")

# curl "${hyrax}${dods_1}?${small_slice}"
# curl "${dars}${dods_1}?${small_slice}"

echo "Simple benchmarks (requests/sec using wrk)"
mkdir -p out

#### DAS
if [[ "$1" == "das" || "$1" == "all" ]]; then
  echo "DAS: ${das_1}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    wrk -d 20 "${dars}${das_1}" | tee out/dars_das_1.wrk | grep 'Requests/sec' | awk '{print $2}' | tee -a out/das_1.rps
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    wrk -d 20 "${thredds}${das_1}" | tee out/thredds_das_1.wrk | grep 'Requests/sec' | awk '{print $2}' | tee -a out/das_1.rps
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    wrk -d 20 "${hyrax}${das_1}" | tee out/hyrax_das_1.wrk | grep 'Requests/sec' | awk '{print $2}' | tee -a out/das_1.rps
  fi

  # collect results
  echo > out/das_1.rps
  grep 'Requests/sec' out/dars_das_1.wrk | awk '{print $2}' >> -a out/das_1.rps
  grep 'Requests/sec' out/thredas_das_1.wrk | awk '{print $2}' >> -a out/das_1.rps
  grep 'Requests/sec' out/hyrax_das_1.wrk | awk '{print $2}' >> -a out/das_1.rps
fi

#### DDS
if [[ "$1" == "dds" || "$1" == "all" ]]; then
  echo "DDS: ${dds_1}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    wrk -d 20 "${dars}${dds_1}" | tee out/dars_dds_1.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    wrk -d 20 "${thredds}${dds_1}" | tee out/thredds_dds_1.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    wrk -d 20 "${hyrax}${dds_1}" | tee out/hyrax_dds_1.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  # collect results
  echo > out/dds_1.rps
  grep 'Requests/sec' out/dars_dds_1.wrk | awk '{print $2}' >> -a out/dds_1.rps
  grep 'Requests/sec' out/thredds_dds_1.wrk | awk '{print $2}' >> -a out/dds_1.rps
  grep 'Requests/sec' out/hyrax_dds_1.wrk | awk '{print $2}' >> -a out/dds_1.rps
fi

#### Small slice
if [[ "$1" == "dods1" || "$1" == "all" ]]; then
  dods="${dods_1}?${small_slice}"
  echo "DODS(small): ${dods}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    wrk -d 20 "${dars}${dods}" | tee out/dars_dods_1_small.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    wrk -d 20 "${thredds}${dods}" | tee out/thredds_dods_1_small.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    wrk -d 20 "${hyrax}${dods}" | tee out/hyrax_dods_1_small.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  # collect results
  echo > out/dods_1_small.rps
  grep 'Requests/sec' out/dars_dods_1_small.wrk | awk '{print $2}' >> -a out/dods_1_small.rps
  grep 'Requests/sec' out/thredds_dods_1_small.wrk | awk '{print $2}' >> -a out/dods_1_small.rps
  grep 'Requests/sec' out/hyrax_dods_1_small.wrk | awk '{print $2}' >> -a out/dods_1_small.rps
fi

#### Big slice
if [[ "$1" == "dods2" || "$1" == "all" ]]; then
  dods="${dods_1}?${big_slice}"
  echo "DODS(big): ${dods}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    wrk -d 60 "${dars}${dods}" | tee out/dars_dods_1_big.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    wrk -d 60 "${thredds}${dods}" | tee out/thredds_dods_1_big.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    wrk -d 60 "${hyrax}${dods}" | tee out/hyrax_dods_1_big.wrk | grep 'Requests/sec' | awk '{print $2}'
  fi

  # collect results
  echo > out/dods_1_big.rps
  grep 'Requests/sec' out/dars_dods_1_big.wrk | awk '{print $2}' >> out/dods_1_big.rps
  grep 'Requests/sec' out/thredds_dods_1_big.wrk | awk '{print $2}' >> -a out/dods_1_big.rps
  grep 'Requests/sec' out/hyrax_dods_1_big.wrk | awk '{print $2}' >> -a out/dods_1_big.rps
fi

# wrk "${dars}${dods_1}?${small_slice}"
# autocannon "${hyrax}${dods_1}?${small_slice}"
# autocannon "${thredds}${dods_1}?${small_slice}"
# echo "DAS1: dars"
# wrk2 -c 100 -t 12 -R 2000 -L "${dars}${das_1}" > out/dars_das_1.wrk2

# sleep 2
# echo "DAS1: hyrax"
# wrk2 -c 100 -t 12 -R 2000 -L "${hyrax}${das_1}" > out/hyrax_das_1.wrk2

# sleep 2
# echo "DAS1: thredds"
# wrk2 -c 100 -t 12 -R 2000 -L "${thredds}${das_1}" > out/thredds_das_1.wrk2
# # wrk2 -c 100 -t 12 -R 2000 -L "${thredds}${das_1}"
# # curl "${thredds}${das_1}"
