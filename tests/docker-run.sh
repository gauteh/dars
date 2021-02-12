cpus=6
root=$(realpath $(dirname $0))
data="${root}/../data"

dcmd="docker run --net=host -it --rm --cpus=${cpus}"

if [ "$1" = "dars" ]; then
  $dcmd --name dars -v "$data:/data:ro" dars -a "0.0.0.0:8001" /data/
elif [ $1 = "hyrax" ]; then
  $dcmd --name hyrax -v "$data:/usr/share/hyrax:ro" opendap/hyrax
elif [ $1 = "thredds" ]; then
  $dcmd --name thredds \
    -v "${root}/../tests/thredds/server.xml:/usr/local/tomcat/conf/server.xml:ro" \
    -v "${data}:/usr/local/tomcat/webapps/thredds/WEB-INF/altContent/startup/public/testdata/data:ro" \
    "unidata/thredds-docker:4.6.14"
else
  echo "no server specified."
fi

