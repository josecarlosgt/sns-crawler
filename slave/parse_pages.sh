#!/bin/bash

# Global Settings

LOG=$(cat ../configuration.json.base | grep -oE '\/.+bash_crawler')

# Constants

CURL_SCRIPT="./ex_curl.sh"
SLEEP_TIME=0.1

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
ID="$CURRENT_SNAME-$CONN"
LOG2_0="$LOG-user-0-$CURRENT_SNAME"
LOG2_1="$LOG-user-1-$CURRENT_SNAME"
# Examples of parameters:

# CONN="followers"
# CONN="following"

# CURRENT_SNAME="AucklandUni"
# CURRENT_SNAME="PedroDuque__"

# Crawlings counter
samplec=0

# SCREEN NAMES TO RETURN
snames=""

###################################################################################### Parse initial list

resp0=$($CURL_SCRIPT $CURRENT_SNAME $CONN main)
echo "**$resp0**" > $LOG2_0 # Logging response
samplec=$(($samplec + 1))
sleep $SLEEP_TIME # Allow some sime to avoid hacker-like behaviour

# Extract user info

if [ $EXTRACT_INFO -eq "1" ]; then
	echo "<user_info>$resp0</user_info>"
fi

# Connections section
echo "<$CONN>"

# Extract screen names

snames0=""
for unpar_name in $(echo $resp0 | grep -oE 'data-screen-name="[a-zA-Z0-9_]+"'); do
	sname=`echo $unpar_name | grep -oE '"[a-zA-Z0-9_]+"' | grep -oE '[a-zA-Z0-9_]+'`
	snames0="$snames0\n$sname"
done
echo "($ID) INITIAL SCREEN NAMES: $snames0" >> $LOG

# Append initial screen names
echo -e $snames0

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

while [ $next -eq 1 ] && ([ $samplec -le $SAMPLE  ] || [ $SAMPLE -lt 0 ]) ; do

resp=$($CURL_SCRIPT $CURRENT_SNAME $CONN xhr $MAX)
echo "**$resp**" > $LOG2_1 # Logging response
samplec=$(($samplec + 1))
sleep $SLEEP_TIME

# Extract screen names

snames1=""
for unpar_name in `echo $resp | grep -oE 'screen-name=\\\\"[a-zA-Z0-9_]+\\\\"'`; do
	sname=`echo $unpar_name | grep -oE '\\\\"[a-zA-Z0-9_]+\\\\"' | grep -oE '[a-zA-Z0-9_]+'`
	snames1="$snames1\n$sname"
done
echo "($ID) SCREEN NAMES: $snames1" >> $LOG

# Append initial screen names
echo -e $snames1

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

done

echo "</$CONN>"

# rm -f $LOG
# echo "REST FOR A WHILE UNTIL NEXT CRAWLING ..." >> $LOG
# sleep $(((${SLEEP_TIME%.*} + 1) * 5))
