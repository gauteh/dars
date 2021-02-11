#! /usr/bin/env bash

set -ep

dars="http://localhost:8001/data/"
thredds="http://localhost:8002/thredds/dodsC/test/data/"
hyrax="http://localhost:8080/opendap/"

das_1="meps_det_vc_2_5km_latest.nc.das"
dds_1="meps_det_vc_2_5km_latest.nc.dds"
dods_1="meps_det_vc_2_5km_latest.nc.dods"
dods_2="coads_climatology.nc4.dods?SST.SST"

function uriencode { jq -nr --arg v "$1" '$v|@uri'; }

big_slice="x_wind_ml.x_wind_ml[1:66][0:64][0][0:28386]"
medium_slice="x_wind_ml.x_wind_ml[1:66][0:10][0][0:10000]"
small_slice="x_wind_ml.x_wind_ml[1:10][0][0][0:1000]"

big_slice=$(uriencode "${big_slice}")
medium_slice=$(uriencode "${medium_slice}")
small_slice=$(uriencode "${small_slice}")

function parse_wrk() {
  # read in
  in="$(</dev/stdin)"

  # Check for errors
  echo "${in}" | grep 'Non-2xx' > /dev/null && echo "-1" && return 1

  echo "${in}" | grep 'Requests/sec' | awk '{print $2}'
}

function parse_ac() {
  jq '.requests.average'
}

tool="wrk --latency"

echo "Simple benchmarks (requests/sec using ${tool})"
mkdir -p out

#### DAS
if [[ "$1" == "das" || "$1" == "all" ]]; then
  echo "DAS: ${das_1}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    $tool -d 20 "${dars}${das_1}" | tee out/dars_das_1.wrk | parse_wrk
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    $tool -d 20 "${thredds}${das_1}" | tee out/thredds_das_1.wrk | parse_wrk
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    $tool -d 20 "${hyrax}${das_1}" | tee out/hyrax_das_1.wrk | parse_wrk
  fi

  # collect results
  : > out/das_1.rps
  cat out/dars_das_1.wrk | parse_wrk >> out/das_1.rps
  cat out/thredds_das_1.wrk | parse_wrk >> out/das_1.rps
  cat out/hyrax_das_1.wrk | parse_wrk >> out/das_1.rps
fi

#### DDS
if [[ "$1" == "dds" || "$1" == "all" ]]; then
  echo "DDS: ${dds_1}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    $tool -d 20 "${dars}${dds_1}" | tee out/dars_dds_1.wrk | parse_wrk
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    $tool -d 20 "${thredds}${dds_1}" | tee out/thredds_dds_1.wrk | parse_wrk
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    $tool -d 20 "${hyrax}${dds_1}" | tee out/hyrax_dds_1.wrk | parse_wrk
  fi

  # collect results
  : > out/dds_1.rps
  cat out/dars_dds_1.wrk | parse_wrk >> out/dds_1.rps
  cat out/thredds_dds_1.wrk | parse_wrk >> out/dds_1.rps
  cat out/hyrax_dds_1.wrk | parse_wrk >> out/dds_1.rps
fi

#### Small slice
if [[ "$1" == "dods1s" || "$1" == "all" ]]; then
  dods="${dods_1}?${small_slice}"
  echo "DODS(small): ${dods}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    $tool -d 20 "${dars}${dods}" | tee out/dars_dods_1_small.wrk | parse_wrk
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    $tool -d 20 "${thredds}${dods}" | tee out/thredds_dods_1_small.wrk | parse_wrk
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    $tool -d 20 "${hyrax}${dods}" | tee out/hyrax_dods_1_small.wrk | parse_wrk
  fi

  # collect results
  : > out/dods_1_small.rps
  cat out/dars_dods_1_small.wrk | parse_wrk >> out/dods_1_small.rps
  cat out/thredds_dods_1_small.wrk | parse_wrk >> out/dods_1_small.rps
  cat out/hyrax_dods_1_small.wrk | parse_wrk >> out/dods_1_small.rps
fi

#### Medium slice
if [[ "$1" == "dods1m" || "$1" == "all" ]]; then
  dods="${dods_1}?${medium_slice}"
  echo "DODS(medium): ${dods}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    $tool -d 20 "${dars}${dods}" | tee out/dars_dods_1_medium.wrk | parse_wrk
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    $tool -d 20 "${thredds}${dods}" | tee out/thredds_dods_1_medium.wrk | parse_wrk
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    $tool -d 20 "${hyrax}${dods}" | tee out/hyrax_dods_1_medium.wrk | parse_wrk
  fi

  # collect results
  : > out/dods_1_medium.rps
  cat out/dars_dods_1_medium.wrk | parse_wrk >> out/dods_1_medium.rps
  cat out/thredds_dods_1_medium.wrk | parse_wrk >> out/dods_1_medium.rps
  cat out/hyrax_dods_1_medium.wrk | parse_wrk >> out/dods_1_medium.rps
fi

#### Big slice
if [[ "$1" == "dods1b" || "$1" == "all" ]]; then
  dods="${dods_1}?${big_slice}"
  echo "DODS(big): ${dods}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    $tool -d 60 --timeout 60 "${dars}${dods}" | tee out/dars_dods_1_big.wrk | parse_wrk
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    ## using wrk2 to limit requests
    wrk2 -d 60 --timeout 60 -R 2 "${thredds}${dods}" | tee out/thredds_dods_1_big.wrk | parse_wrk
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    $tool -d 60 --timeout 60 "${hyrax}${dods}" | tee out/hyrax_dods_1_big.wrk | parse_wrk
  fi

  # collect results
  : > out/dods_1_big.rps
  cat out/dars_dods_1_big.wrk | parse_wrk  >> out/dods_1_big.rps
  cat out/thredds_dods_1_big.wrk | parse_wrk >> out/dods_1_big.rps
  cat out/hyrax_dods_1_big.wrk | parse_wrk  >> out/dods_1_big.rps
fi

#### DODS2
if [[ "$1" == "dods2" || "$1" == "all" ]]; then
  dods="${dods_2}"
  echo "DODS2(medium): ${dods}"

  if [[ "$2" == "dars" || "$2" == "all" ]]; then
    echo -n "dars: "
    sleep 2
    $tool -d 20 "${dars}${dods}" | tee out/dars_dods_2.wrk | parse_wrk
  fi

  if [[ "$2" == "thredds" || "$2" == "all" ]]; then
    echo -n "thredds: "
    sleep 2
    $tool -d 20 "${thredds}${dods}" | tee out/thredds_dods_2.wrk | parse_wrk
  fi

  if [[ "$2" == "hyrax" || "$2" == "all" ]]; then
    echo -n "hyrax: "
    sleep 2
    $tool -d 20 "${hyrax}${dods}" | tee out/hyrax_dods_2.wrk | parse_wrk
  fi

  # collect results
  : > out/dods_2.rps
  cat out/dars_dods_2.wrk | parse_wrk >> out/dods_2.rps
  cat out/thredds_dods_2.wrk | parse_wrk >> out/dods_2.rps
  cat out/hyrax_dods_2.wrk | parse_wrk >> out/dods_2.rps
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
