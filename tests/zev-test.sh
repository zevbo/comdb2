#!/bin/sh


on_first=1
for var in "$@"
do
	if [ $on_first == 0 ]
	then
		cdb2sql $1 -f $var.sql > temp-result.txt
		result_diff=$(diff temp-result.txt $var.expected)
		if [ "$result_diff" != "" ]
		then
			echo "\033[1m$var failed with following diff:\033[0m"
			diff temp-result.txt $var.expected
			break	
		else
			echo "\033[1m$var passed\033[0m"
		fi
		echo ""

	else
		on_first=0
	fi
done	
