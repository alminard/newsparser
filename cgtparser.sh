#!/bin/sh

HOMEDIR=./

CLASSPATH="./:$HOMEDIR/lib/newsarchive1.0.jar:$HOMEDIR/lib/jericho-html-3.3.jar:$HOMEDIR/lib/jdom.jar:$HOMEDIR/lib/commons-compress-1.2.jar"
export CLASSPATH

cmd="java -Dfile.encoding=UTF8 -ms256m -mx512m fbk.hlt.utility.archive.CGTParser $*" 

runcmd=1

if [ $# = 0 ]; then
	runcmd=0
fi
	
for opt in $cmd
do
	if [ $opt = "-h" ]; then
		runcmd=0
    	break
    elif [ $opt = "--help" ]; then
    	runcmd=0
    	break
	fi
done

if [ $runcmd = 0 ]; then
	echo "Usage: cgtparser [OPTION] <FILE.cgt>"
	java fbk.hlt.utility.archive.CGTParser -ho
else 
	$cmd
fi
