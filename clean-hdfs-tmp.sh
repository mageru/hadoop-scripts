#!/bin/bash

OPTIND=1

usage() { echo "Usage: $0 [-n <namenode hostname>] [-d <files over than this number of days>]" 1>&2; exit 1; }
namenode=
days=
while getopts "n:d:" OPTION; do
    case "${OPTION}" in
        n)
            namenode=${OPTARG}
            ;;
        d)
            days=${OPTARG}
            ;;
        ?)
            usage
            ;;
    esac
done
shift $((OPTIND-1))
echo "namenode = ${namenode}"
echo "days = ${days}"


if [ -z "${namenode}" ] || [ -z "${days}" ]; then
    usage
fi

now=$(date +%s)
curl "http://${namenode}:50070/getimage?getimage=1&txid=latest" > /tmp/img.dump
hdfs oiv -i /tmp/img.dump -o /tmp/fsimage.txt
echo ""> /tmp/files_to_delete
cat /tmp/fsimage.txt | grep -v "^d" | awk '$8 ~ /^\/tmp\//' | while read f; do
  dir_date=`echo $f | awk '{print $6}'`
  difference=$(( ( $now - $(date -d "$dir_date" +%s) ) / (24 * 60 * 60 ) ))
  if [ $difference -gt $days ]; then
    echo $f
    echo $f | awk '{print $8}' >>/tmp/files_to_delete
  fi
done

hdfsdel="sudo -u hdfs hdfs dfs -rm -f -skipTrash"
COUNTER=1
BATCHSIZE=1000

END=$(wc -l < /tmp/files_to_delete)
while [ $COUNTER -lt $END ]; do
	LINEEND=`expr $COUNTER + $BATCHSIZE`
	if [[ $END -le $LINEEND ]]; then
		LINEEND=$END
	fi
		
	FILES=$(sed -n "${COUNTER},${LINEEND}p" /tmp//files_to_delete | awk '
	{ 
		for (i=1; i<=NF; i++)  {
			a[NR,i] = $i
		}
	}
	NF>p { p = NF }
	END {    
		for(j=1; j<=p; j++) {
			str=a[1,j]
			for(i=2; i<=NR; i++){
				str=str" "a[i,j];
			}
			print str
		}
	}')
	echo "Delete command"
	${hdfsdel} ${FILES}
	let COUNTER=COUNTER+${BATCHSIZE}
done

# CLEAN UP TMP FILES
if [ -e /tmp/img.dump ]; then
	echo "Removing /tmp/img.dump"
	rm -f /tmp/img.dump
else 
	echo "File not found: /tmp/img.dump"
fi

# CLEAN UP TMP FILES
if [ -e /tmp/fsimage.txt ]; then
	echo "Removing /tmp/fsimage.txt"
	rm -f /tmp/fsimage.txt
else 
	echo "File not found: /tmp/fsimage.txt"
fi

if [ -e /tmp/files_to_delete ]; then
	echo "Removing /tmp/files_to_delete"
	rm -f /tmp/files_to_delete
else 
	echo "File not found: /tmp/files_to_delete"
fi
