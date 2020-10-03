To run the files start the necessary datanode and name node daemons to get HDFS up and running. Then you need to run these files on a machine that has Apache Hadoop installed.
Run the following command for each of the .jar and .java files. The .jar consists of the classes that are put in a tarball after the .java file is compiled.

For example:

$ Hadoop jar hashtag.jar topten_hashtags /homework1/training_set_tweets.txt hashtags

hashtag.jar - the name of the jar file that will be run
topten_hashtags - the name of the file with the source

The third and fourth command line argument is the input file location and the output file location in HDFS.

This is the command to run the fifth question and the sixth question.

For the seventh question you need to include the users file as input as well as below.

$ Hadoop jar hashtag.jar topten_hashtags /homework1/training_set_tweets.txt /homework1/training_set_users.txt hashtags
