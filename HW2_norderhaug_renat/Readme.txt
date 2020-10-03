
Change directory into any of the folders within this zip file. In the folder you first run "set package", this will build the jar containing my application.
Next to run the application using the following command:

spark-submit --class "AssignmentOne" assignmentone_2.12-1.0.jar /homework1/training_set_tweets.txt

Repeat this process in each folder and make sure to change the names of the jars to the names in the assignment 

AssignmentTwo:

spark-submit --class "AssignmentTwo" assignmenttwo_2.12-1.0.jar /homework1/training_set_tweets.txt

AssignmentThree:

spark-submit --class "AssignmentThree" assignmentthree_2.12-1.0.jar /homework1/training_set_tweets.txt /homework1/training_set_users.txt

HW #2 project written in Scala, the project includes code to the 10 most popular mentions, 10 most retweeted users, and 10 most tweeted at users in a large Twitter dataset.
