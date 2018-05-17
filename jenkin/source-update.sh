if ls -1qA ./spark | grep -q .  
then
	cd ./spark
	git pull origin snappy/branch-2.1
    cd -
else
	git clone git@github.com:SnappyDataInc/spark.git ./spark/
fi


if ls -1qA ./spark-jobserver | grep -q .  
then
	cd ./spark-jobserver
	git pull origin snappydata
    cd -
else
    git clone git@github.com:SnappyDataInc/spark-jobserver.git ./spark-jobserver/
fi


if ls -1qA ./store | grep -q .  
then
	cd ./store
	git pull origin snappy/master
    cd -
else
	git clone git@github.com:SnappyDataInc/snappy-store.git ./store/
fi


if ls -A ./aqp | grep -q .  
then
    cd ./aqp 
    git pull origin master
    cd -
else
	git clone git@github.com:SnappyDataInc/snappy-aqp.git ./aqp/	
fi


if ls -A ./snappy-connector | grep -q .  
then
	cd ./snappy-connector
    git pull origin master 
    cd -
else
	git clone git@github.com:SnappyDataInc/spark.git ./snappy-connector/
fi
  
