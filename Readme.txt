Install Redis Client

Start server
redis-cli


Arguments to be passed during time

1. First argument args[0]
beforeFailOver boolean - { true [For the first run]
			 {false [for the next hourly runs, loads id's from file and continues from where it was stopped]

2. Second Argument args[1]
SubscriptionKeyLimit - Pass 0 for the 1st run 
		     - Pass as per Subscription key count written in the "subscriptionKeyMaintaining.csv" for the next runs.

Note - *Do not run if Subscription keycount has reached 200K*

3.Third Argument args[2]
NUM_HOPS - Pass 0 during 1st run 
	 - For next runs check for Log file/IdsToVisitInCurrentHop.csv file and provide the HOP count

3.Fourth Argument args[3]
NUM_HOPS - Pass flase during 1st run 
	 - For next runs check if Redis fails data crashed, load from Backup files.

So the arguments for the 1st crawl should be

true 0 0 false

Second and subsequent crawls will be

false SubKeyUpdated HopCountUpdated 

-----------------------------------------------------------------------------------------------------------------------------------------

Things to be taken care of, during every hourly run

Make a copy of these files.

*idsToVisitInCurrentHop.csv
*idsVisited.csv
*idsToVisitNextHop.csv

Beacuse, once Data is loaded onto lists from files, after first hourly crawl the files are deletd and new file with backup data after 45 minutes 
will be written on to file with same names as previous with next set of PaperIds.

Repeat after every crawl