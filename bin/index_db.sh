#!/bin/bash
#+------------------------------------------------------------------------------------------------------------------------------+
#| DBpedia Spotlight - Create database-backed model                                                                             |
#| @author Joachim Daiber                                                                                                       |
#+------------------------------------------------------------------------------------------------------------------------------+

# $1 Working directory
# $2 Locale (en_US)
# $3 Stopwords file
# $4 Analyzer+Stemmer language prefix e.g. Dutch
# $5 Model target folder

export MAVEN_OPTS="-Xmx26G"

usage ()
{
     echo "index_db.sh"
     echo "usage: ./index_db.sh -o /data/spotlight/nl/opennlp wdir nl_NL /data/spotlight/nl/stopwords.nl.list Dutch /data/spotlight/nl/final_model"
     echo "Create a database-backed model of DBpedia Spotlight for a specified language."
     echo " "
}


opennlp="None"
eval="false"
#TODO Modifying this to only get Data instead of calling Spotlight. Change Local_Mode as well
data_only="true"
local_mode="true"


while getopts "ledo:" opt; do
  case $opt in
    o) opennlp="$OPTARG";;
    e) eval="true";;
    d) data_only="true";;
    l) local_mode="true"
  esac
done


shift $((OPTIND - 1))

if [ $# != 5 ]
then
    usage
    exit
fi

BASE_DIR=$(pwd)

if [[ "$1"  = /* ]]
then
   BASE_WDIR="$1"
else
   BASE_WDIR="$BASE_DIR/$1"
fi

if [[ "$5" = /* ]]
then
   TARGET_DIR="$5"
else
   TARGET_DIR="$BASE_DIR/$5"
fi

if [[ "$3" = /* ]]
then
   STOPWORDS="$3"
else
   STOPWORDS="$BASE_DIR/$3"
fi

WDIR="$BASE_WDIR/$2"

if [[ "$opennlp" == "None" ]]; then
    echo "";
elif [[ "$opennlp" != /* ]]; then
    opennlp="$BASE_DIR/$opennlp"; 
fi


LANGUAGE=`echo $2 | sed "s/_.*//g"`

echo "Language: $LANGUAGE"
echo "Working directory: $WDIR"

mkdir -p $WDIR

#Download:
echo "Downloading DBpedia dumps..."
cd $WDIR
if [ ! -f "redirects.nt" ]; then
  curl -# http://downloads.dbpedia.org/current/core-i18n/$LANGUAGE/redirects_$LANGUAGE.nt.bz2 | bzcat > redirects.nt
  curl -# http://downloads.dbpedia.org/current/core-i18n/$LANGUAGE/disambiguations_$LANGUAGE.nt.bz2 | bzcat > disambiguations.nt
  curl -# http://downloads.dbpedia.org/current/core-i18n/$LANGUAGE/instance_types_$LANGUAGE.nt.bz2 | bzcat > instance_types.nt
fi


if [ "$DATA_ONLY" != "true" ]; then

  #Set up Spotlight:
  cd $BASE_WDIR
  
  if [ -d dbpedia-spotlight ]; then
      echo "Updating DBpedia Spotlight..."
      cd dbpedia-spotlight
      git reset --hard HEAD
      git pull
      mvn -T 1C -q clean install
  else
      echo "Setting up DBpedia Spotlight..."
      #TODO - To change to actual DBpedia spotlight repo.
      git clone -b feature/scala-2.10 https://github.com/naveenmadhire/dbpedia-spotlight
      #git clone --depth 1 https://github.com/dbpedia-spotlight/dbpedia-spotlight.git
      cd dbpedia-spotlight
      mvn -T 1C -q clean install
  fi

fi

cd $BASE_WDIR

#Set up DBPedia WikiStats:
if [ -d $BASE_WDIR/wikistats ]; then
    cd $BASE_WDIR/wikistats/wikipedia-stats-extractor
    git reset --hard HEAD
    git pull
    mvn -T 1C -q assembly:assembly -Dmaven.test.skip=true
else
    echo "Setting up Json WikiPedia Repo"
    #Using Temporary Repo
    git clone https://github.com/naveenmadhire/json-wikipedia-dbspotlight
    cd json-wikipedia-dbspotlight
    mvn -T 1C -q install -Dmaven.test.skip=true
    echo "Setting up WikiStats Repo..."
    mkdir -p $BASE_WDIR/wikistats/
    cd $BASE_WDIR/wikistats/
    #git clone --depth 1 https://github.com/dbpedia-spotlight/wikipedia-stats-extractor
    git clone --depth 1 https://github.com/naveenmadhire/wikipedia-stats-extractor
    cd wikipedia-stats-extractor
    echo "Building WikiStats Repo"
    mvn -T 1C -q install -Dmaven.test.skip=true
fi


# Stop processing if one step fails
set -e

if [ "$local_mode" == "true" ]; then

  #TODO - Removing the CURL for time being
  echo "Downloading the wiki file"
  curl -# "http://dumps.wikimedia.org/${LANGUAGE}wiki/latest/${LANGUAGE}wiki-latest-pages-articles.xml.bz2" | bzcat > $WDIR/${LANGUAGE}wiki-latest-pages-articles.xml

else
  #Load the dump into HDFS:

  if hadoop fs -test -e ${LANGUAGE}wiki-latest-pages-articles.xml ; then
    echo "Dump already in HDFS."
  else
    echo "Loading Wikipedia dump into HDFS..."
    if [ "$eval" == "false" ]; then
        curl -# "http://dumps.wikimedia.org/${LANGUAGE}wiki/latest/${LANGUAGE}wiki-latest-pages-articles.xml.bz2" | bzcat | hadoop fs -put - ${LANGUAGE}wiki-latest-pages-articles.xml
    else
        curl -# "http://dumps.wikimedia.org/${LANGUAGE}wiki/latest/${LANGUAGE}wiki-latest-pages-articles.xml.bz2" | bzcat | python $BASE_WDIR/pig/pignlproc/utilities/split_train_test.py 12000 $WDIR/heldout.txt | hadoop fs -put - ${LANGUAGE}wiki-latest-pages-articles.xml
    fi
  fi

fi



#Load the stopwords into HDFS:
echo "Moving stopwords into HDFS..."
cd $BASE_DIR




if [ "$local_mode" == "false" ]; then

  hadoop fs -put $STOPWORDS stopwords.$LANGUAGE.list || echo "stopwords already in HDFS"

  if [ -e "$opennlp/$LANGUAGE-token.bin" ]; then
      hadoop fs -put "$opennlp/$LANGUAGE-token.bin" "$LANGUAGE.tokenizer_model" || echo "tokenizer model already in HDFS"
  else
      touch empty;
      hadoop fs -put empty "$LANGUAGE.tokenizer_model" || echo "tokenizer model already in HDFS"
      rm empty;
  fi

else

  cd $WDIR
  cp $STOPWORDS stopwords.$LANGUAGE.list || echo "stopwords already in HDFS"

  if [ -e "$opennlp/$LANGUAGE-token.bin" ]; then
      cp "$opennlp/$LANGUAGE-token.bin" "$LANGUAGE.tokenizer_model" || echo "tokenizer already exists"
  else
      touch "$LANGUAGE.tokenizer_model"
  fi

fi


#Adapt pig params:
cd $BASE_DIR
cd $1/wikistats/wikipedia-stats-extractor

WIKISTATS_JAR="$WDIR/wikistats/wikipedia-stats-extractor/target/wikipedia-stats-extractor-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ "$local_mode" == "true" ]; then

  INPUT="$WDIR/${LANGUAGE}wiki-latest-pages-articles.xml"
  STOPWORDS="$WDIR/stopwords.$LANGUAGE.list"
  echo "Downloading the latest spark version"

  if [ -d $WDIR/spark-1.4.1-bin-hadoop2.6 ]; then
    echo "Spark directory already present"
  else
    echo "Downloading the Spark directory from the Apache Spark website"
    curl -# "http://apache.mirrorcatalogs.com/spark/spark-1.4.1/spark-1.4.1-bin-hadoop2.6.tgz"  > $WDIR/spark-1.4.1-bin-hadoop2.6.tgz
    tar -xvf $WDIR/spark-1.4.1-bin-hadoop2.6.tgz
  fi

  cd $WDIR/spark-1.4.1-bin-hadoop2.6/bin
  ./spark-submit --class org.dbpedia.spotlight.wikistats.main --master local[*] --conf spark.sql.shuffle.partitions=50 $WIKISTATS_JAR $INPUT $STOPWORDS ${LANGUAGE} $WDIR/ $4Stemmer

else

  INPUT="/user/$USER/${LANGUAGE}wiki-latest-pages-articles.xml"
  STOPWORDS="/user/$USER/stopwords.$LANGUAGE.list"

  ./spark-submit --class org.dbpedia.spotlight.wikistats.main --master localhost --conf spark.sql.shuffle.partitions=50 $WIKISTATS_JAR $INPUT $STOPWORDS ${LANGUAGE} $WDIR/ $4Stemmer
fi

#Copy results to local:
cd $BASE_DIR
cd $WDIR

if [ "$local_mode" == "true" ]; then

  cat ./TokenCounts/part* > tokenCounts
  cat ./PairCounts/part* > pairCounts
  cat ./UriCounts/part* > uriCounts
  cat ./TotalSfCounts/part* > sfAndTotalCounts

else

  hadoop fs -cat ./TokenCounts/part* > tokenCounts
  hadoop fs -cat ./PairCounts/part* > pairCounts
  hadoop fs -cat ./UriCounts/part* > uriCounts
  hadoop fs -cat ./TotalSfCounts/part* > sfAndTotalCounts

fi


#Create the model:
cd $BASE_DIR
cd $1/dbpedia-spotlight

CREATE_MODEL="mvn -pl index exec:java -Dexec.mainClass=org.dbpedia.spotlight.db.CreateSpotlightModel -Dexec.args=\"$2 $WDIR $TARGET_DIR $opennlp $STOPWORDS $4Stemmer\";"

if [ "$data_only" == "true" ]; then
    echo "$CREATE_MODEL" >> create_models.job.sh
else
  eval "$CREATE_MODEL"
  
  if [ "$eval" == "true" ]; then
      mvn -pl eval exec:java -Dexec.mainClass=org.dbpedia.spotlight.evaluation.EvaluateSpotlightModel -Dexec.args="$TARGET_DIR $WDIR/heldout.txt" > $TARGET_DIR/evaluation.txt
  fi
fi

echo "Finished!"
set +e
