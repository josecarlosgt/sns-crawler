#!/bin/bash

# Global Settings

LOG=$(cat ../configuration.json.base | grep -oE '\/.+bash_crawler')

# Constants

CURL_SCRIPT="./shell-crawler/ex_curl.sh"
SLEEP_TIME=0.05
ATTEMPTS=5
ATTEMPTS_INTERVAL=3

# Parameters

CONN=""
CURRENT_SNAME=""
SAMPLE="-1"
EXTRACT_INFO=0
if [ $# -ge 3 ]; then
	CONN=$1; shift
	CURRENT_SNAME=$1; shift
	PSAMPLE=$1
	if [ -n "$PSAMPLE" ]; then
		 SAMPLE=$PSAMPLE
	fi
else
	echo "Invalid arguments. Example: " >> $LOG
	echo "./parse_pages.sh followers PedroDuque__ [100]|-1" >> $LOG
	exit 0
fi

echo "PARAMETERS: connection type=<$CONN>, screen name=<$CURRENT_SNAME>, sample=<$SAMPLE> . Ready to proceed ..." >> $LOG
ID="$CURRENT_SNAME-$CONN"

# Examples of parameters:

# CONN="followers"
# CONN="following"

# CURRENT_SNAME="AucklandUni"
# CURRENT_SNAME="PedroDuque__"

# Crawlings counter: Start at one since first attempt to retrieve base page counts
samplec=1

# SCREEN NAMES TO RETURN
snames=""

###################################################################################### Parse initial list

resp0=$($CURL_SCRIPT $CURRENT_SNAME $CONN main)
sleep $SLEEP_TIME # Allow some sime to avoid hacker-like behaviour

# Extract screen names

snames0=""
for unpar_name in $(echo $resp0 | grep -oE 'data-screen-name="[a-zA-Z0-9_]+"'); do
	sname=`echo $unpar_name | grep -oE '"[a-zA-Z0-9_]+"' | grep -oE '[a-zA-Z0-9_]+'`
	#if [ -n "$snames0" ]; then
	snames0="$snames0,$sname"
	#else
	#	snames0="$sname"
	#fi
done
echo "($ID) INITIAL SCREEN NAMES: $snames0" >> $LOG

# Append initial screen names
echo -n $snames0

# Extract pagination offset (min position to be max position in the next call, if has more connections)

unpar_min_pos0=$(echo $resp0 | grep -oE 'data-min-position="[0-9]+"')
min_pos0=$(echo $unpar_min_pos0 | grep -oE '[0-9]+')
echo "($ID) INITIAL MIN POSITION: $min_pos0" >> $LOG

# Set pagination
MAX=$min_pos0

################################################################################## Crawl connections

next=1
if [ -n "$MAX" ]; then
	echo "($ID) INITIAL COUNT: $samplec OUT OF $SAMPLE. EXECUTING CRAWLER." >> $LOG
else
	echo "($ID) INITIAL COUNT: $samplec OUT OF $SAMPLE. NO MORE CONNECTIONS." >> $LOG
	next=0
fi

while [ $next -eq 1 ] && ([ $samplec -lt $SAMPLE  ] || [ $SAMPLE -lt 0 ]) ; do

countAttempts=1
successfulAttempt=0
failedAttempt=0
resp=""
while [ $successfulAttempt -eq 0 ] && [ $failedAttempt -eq 0 ] ; do
	resp=$($CURL_SCRIPT $CURRENT_SNAME $CONN xhr $MAX)
	sleep $SLEEP_TIME

	if [ -n "$resp" ]; then
		successfulAttempt=1
		samplec=$(($samplec + 1))
	else
		if [ $countAttempts -ge $ATTEMPTS ]; then
			failedAttempt=1
			echo "($ID) EMPTY RESPONSE: $countAttempts out of $ATTEMPTS" >> $LOG
		else
			echo "($ID) EMPTY RESPONSE. ATTEMPTING ONE MORE TIME: $countAttempts out of $ATTEMPTS" >> $LOG
			countAttempts=$(($countAttempts + 1))
			sleep $ATTEMPTS_INTERVAL
		fi
	fi
done

# Extract screen names
if [ $failedAttempt -eq 0 ]; then
	snames1=""
	for unpar_name in `echo $resp | grep -oE 'screen-name=\\\\"[a-zA-Z0-9_]+\\\\"'`; do
		sname=`echo $unpar_name | grep -oE '\\\\"[a-zA-Z0-9_]+\\\\"' | grep -oE '[a-zA-Z0-9_]+'`
		#if [ -n "$snames1" ]; then
		snames1="$snames1,$sname"
		#else
		#	snames1="$sname"
		#fi
	done
	echo "($ID) SCREEN NAMES: $snames1" >> $LOG

	# Append initial screen names
	echo -n $snames1

	# Has more connections?

	unpar_has_more=$(echo $resp | grep -oE '"has_more_items":(true|false)')
	has_more=$(echo $unpar_has_more | grep -oE '(true|false)')
	echo "($ID) HAS MORE: $has_more" >> $LOG

	# Extract pagination offset (min position to be max position in the next call, if has more connections)

	unpar_min_pos=$(echo $resp | grep -oE '"min_position":"[0-9]+"')
	min_pos=$(echo $unpar_min_pos | grep -oE '[0-9]+')
	echo "($ID) MIN POSITION: $min_pos" >> $LOG

	if [ -n "$has_more" ] ; then
		if [ $has_more = "true" ]; then
			MAX=$min_pos
		else
			next=0
		fi
	else
		next=0
	fi

else
	next=0
fi
done

# rm -f $LOG
# echo "REST FOR A WHILE UNTIL NEXT CRAWLING ..." >> $LOG
# sleep $(((${SLEEP_TIME%.*} + 1) * 5))
