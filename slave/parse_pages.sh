#!/bin/bash

# Global Settings

LOG=$(cat ../configuration.json | grep -oE '\/.+crawler_')$(date +'%d_%m_%Y')

# Constants

CURL_SCRIPT="./ex_curl.sh"
SLEEP_TIME=0.05

# Parameters

CONN=""
CURRENT_SNAME=""
SAMPLE="-1"
EXTRACT_INFO=0
if [ $# -ge 3 ]; then
	CONN=$1; shift
	CURRENT_SNAME=$1; shift
	EXTRACT_INFO=$1; shift
	PSAMPLE=$1
	if [ -n "$PSAMPLE" ]; then
		 SAMPLE=$PSAMPLE
	fi
else
	echo "Invalid arguments. Example: " >> $LOG
	echo "./parse_pages.sh followers PedroDuque__ 0|1 [100]" >> $LOG
	exit 0
fi

echo "PARAMETERS: connection type=<$CONN>, screen name=<$CURRENT_SNAME>, extract info=<$EXTRACT_INFO> sample=<$SAMPLE> . Ready to proceed ..." >> $LOG

# Examples of parameters:

# CONN="followers"
# CONN="following"

# CURRENT_SNAME="AucklandUni"
# CURRENT_SNAME="PedroDuque__"

# Sample counter
samplec=1

# SCREEN NAMES TO RETURN
snames=""

###################################################################################### Parse initial list

resp0=$($CURL_SCRIPT $CURRENT_SNAME $CONN main)
sleep $SLEEP_TIME # Allow some sime to avoid hacker-like behaviour

# Extract user info

if [ $EXTRACT_INFO -eq "1" ]; then
	echo "<user_info>$resp0</user_info>"
fi

# Connections section
echo "<$CONN>"

# Extract screen names

snames0=""
for unpar_name in $(echo $resp0 | grep -oE 'data-screen-name="[a-zA-Z0-9]+"'); do
	sname=`echo $unpar_name | grep -oE '"[a-zA-Z0-9]+"' | grep -oE '[a-zA-Z0-9]+'`
	snames0="$snames0\n$sname"
	samplec=$(($samplec + 1))
done
echo "INITIAL SCREEN NAMES: $snames0" >> $LOG

# Append initial screen names
echo -e $snames0

# Extract pagination offset (min position to be max position in the next call, if has more connections)

unpar_min_pos0=$(echo $resp0 | grep -oE 'data-min-position="[0-9]+"')
min_pos0=$(echo $unpar_min_pos0 | grep -oE '[0-9]+')
echo "INITIAL MIN POSITION: $min_pos0" >> $LOG

# Set pagination
MAX=$min_pos0

################################################################################## Crawl connections

next=1
echo "SAMPLE COUNT: EXTRACTED $samplec OUT OF $SAMPLE" >> $LOG
while [ $next -eq 1 ] && ([ $samplec -le $SAMPLE  ] || [ $SAMPLE -lt 0 ]) ; do

samplec=$(($samplec + 1))

resp=$($CURL_SCRIPT $CURRENT_SNAME $CONN xhr $MAX)
sleep $SLEEP_TIME

# Extract screen names

snames1=""
for unpar_name in `echo $resp | grep -oE 'screen-name=\\\\"[a-zA-Z0-9]+\\\\"'`; do
	sname=`echo $unpar_name | grep -oE '\\\\"[a-zA-Z0-9]+\\\\"' | grep -oE '[a-zA-Z0-9]+'`
	snames1="$snames1\n$sname"
done
echo "SCREEN NAMES: $snames1" >> $LOG

# Append initial screen names
echo -e $snames1

# Has more connections?

unpar_has_more=$(echo $resp | grep -oE '"has_more_items":(true|false)')
has_more=$(echo $unpar_has_more | grep -oE '(true|false)')
echo "HAS MORE: $has_more" >> $LOG

# Extract pagination offset (min position to be max position in the next call, if has more connections)

unpar_min_pos=$(echo $resp | grep -oE '"min_position":"[0-9]+"')
min_pos=$(echo $unpar_min_pos | grep -oE '[0-9]+')
echo "MIN POSITION: $min_pos" >> $LOG

if [ -n "$has_more" ] ; then
	if [ $has_more = "true" ]; then
		MAX=$min_pos
	else
		next=0
	fi
else
	next=0
fi

done

echo "</$CONN>"

# rm -f $LOG
# echo "REST FOR A WHILE UNTIL NEXT CRAWLING ..." >> $LOG
# sleep $(((${SLEEP_TIME%.*} + 1) * 5))