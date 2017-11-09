Install Redis Client

Start server
redis-cli
SET CONFIG 


Arguments to be passed during time

1. First argument args[0]
isFirstCrawl boolean - { true [For the first run]
			 {false [for the next hourly runs, loads id's from file and continues from where it was stopped]

2. Second Argument args[1]
SubscriptionKeyLimit - Pass 0 for the 1st run 
		     - Pass as per Subscription key count written in the "subscriptionKeyMaintaining.csv" for the next runs.

Note - *Do not run if Subscription keycount has reached 200K*

3.Third Argument args[2]
NUM_HOPS - Pass 0 during 1st run 
	 - For next runs check for Log and provide the HOP count

3.Fourth Argument args[3]
loadformfilesRequired boolean   - Pass flase during 1st run 
	       			- pass true only if Redis data crashes, load from Backup files.

So the arguments for the 1st crawl should be

true 0 0 false

Second and subsequent crawls will be

false SubKeyUpdated HopCountUpdated loadformfilesRequired 


