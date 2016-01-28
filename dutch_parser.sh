#!/bin/tcsh

# compile with:
# $ javac -cp . -d classes/ -sourcepath src/ src/main/java/org/fbk/hlt/newsreader/ZipManager.java 

# example of running
# $ ./dutch_parser.sh DutchHouse-original-XML.zip DutchHouse.cgt
# 
# then I can have a mirror of the original XML files in NAF output file using:
# $ ./cgtparser -m -O /tmp/DutchNAF/ DutchHouse.cgt

$JAVA_HOME/bin/java -Dfile.encoding=UTF8 -cp "lib/newsarchive1.0.jar:lib/jdom.jar" main.java.org.fbk.hlt.newsreader.DutchParlamentParser $*

