#/bin/sh

# This script parses the log generated from analysing new followers over time.
# The purpose of this script is to generate the location of each new follower
# to create clusters that show how new followers are being added from different
# locations across the world.

IFS=$'\n'
intermediaries=~/output/d_intermediaries/*
locations=~/output/d_followers_location/
for time_f in $intermediaries; do
	echo "$time_f"
	time_name=`echo $time_f | grep -oE 'intermediaries\-.+$'`
	time_name=$locations`echo ${time_name/intermediaries/locations}`
	echo $time_name	
	> $time_name
	for follower in $(grep "NEW FOLLOWER" $time_f); do
		# echo $follower
		location=`echo $follower | grep -oE '@\[.*\]' | grep -oE '\[.*\]'`
		location=${location#"["}
		location=${location%"]"}
		if [ -n "$location" ]; then
			echo "$location" >> $time_name
		fi
	done
done

