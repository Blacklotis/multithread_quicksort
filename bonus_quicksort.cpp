/** This program illustrates the server end of the message queue **/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <signal.h>
#include <vector>
#include <list>
#include <string>
#include <fstream>
#include <iostream> 

using namespace std;

struct position
{
	int low;
	int high;
};

/* The record in the hash table */
struct record
{
	/* The record id */
	int id;

	/* The first name */	
	string firstName;

	/* The first name */
	string lastName;	
};



/**
 * The structure of a hashtable cell
 */
class hashTableCell
{
	/* Public members */
	public:	

		/**
		 * Initialize the mutex
		 */
		hashTableCell()
		{
			pthread_mutex_init(&mutex, NULL);
			/* TODO: Initialize the mutex (which you will declare at the end of this
			 * class; please see the buttom of the class) using pthread_mutex_init()
			 */
		}

		/**
		 * TODO: Deallocate the mutex declared at the buttom. The function to use 
		 * is pthread_mutex_destroy. E.g., pthread_mutex_destroy(&myMutex) where
		 * myMutex is the name of the mutex.       
		 */
		~hashTableCell()
		{
			/* Deallocate the mutex using pthread_mutex_destroy() */
				pthread_mutex_destroy(&mutex);
		}

		/**
		 * Locks the cell mutex
		 */
		void lockCell()
		{
			/* TODO: Add code for locking the cell mutex e.g., 
			 * declated at the buttom of this class 
			 * e.g., pthread_mutex_lock(& <mutex name>)          
			 */
			pthread_mutex_lock(&mutex);
		}

		/**
		 * Unlocks the cell mutex
		 */
		void unlockCell()
		{
			/* TODO: Add code for unlocking the cell mutex
			 * declared at the buttom of this class.
			 * e.g., pthread_mutex_unlock(&<mutex name>) 
			 */
			pthread_mutex_unlock(&mutex);
		}



		//each cell has an individual record.
		record rec;
		pthread_mutex_t mutex;


		/**
		 * TODO: declare a mutex. E.g., pthread_mututex_t myMutex.
		 * The assignment description contains references to more detailed
		 * documentation.               
		 */

};

/* The number of cells in the hash table */
#define NUMBER_OF_HASH_CELLS 100

/* The number of inserter threads */
#define NUM_INSERTERS 5

/* The hash table */
vector<hashTableCell> hashTable(NUMBER_OF_HASH_CELLS);

/* The number of threads */
int numThreads = 0;

bool flag = false;
vector<pthread_t> inserterTids;

/* The client will send messages to the server to request certain ids (please
 * see the code for the client). When the server receives a request (which is
 * just an integer representing an ID of the requested record), the server will
 * save it on this queue, which represents a queue of requests that the server
 * must service. 
 */
list<position> positionLookUpList;
pthread_mutex_t lookUpMutex;

/**
 * TODO: Declare and initialize a mutex for protecting the idsToLookUpList.
 *
 *  Rationale:  
 * As you will see below, the idsToLookUpList will be accessed by multiple threads
 * that will be servicing client's requests. Specifically, one thread 
 * (the main thread) will be receiving the ids of the requested records from the 
 * client, adding the received ids to the idsToLookUpList list (by calling idsToLookUpList.push_back(<the received id>)),
 * and multiple threads will be removing the ids from this list in order to look them
 * up and reply to the client. Hence, we need a mutex that will protect idsToLookUpList
 * against race conditions.   
 */

pthread_cond_t threadPoolCond;
/**
 * TODO: declare and initialize the condition variable, threadPoolCondVar, 
 * for implementing a thread pool.  E.g., pthread_cond_t <name of the condition variable> = 
 * PTHREAD_COND_INITIALIZER. This variable will serve the purpose of the thread pool.
 * 
 * Rationale:
 * We will have a group of threads (sometimes referred to as worker threads),
 * whose job it will be to sleep until woken up by the main thread. When woken up, 
 * the worker thread will remove a record id it from the idsToLookUpList list, look up 
 * the id in the hash table, and send the record back to the server. The main
 * thread will wake up the worker thread when it receives an id of the requested 
 * record.  The purpose of the condition variable is to provide a place for a 
 * worker thread to sleep until it is woken up by the main thread; another words,
 * to serve as basis for implementing the thread pool.          
 */

/* TODO: Declare the mutex, threadPoolMutex, for protecting the condition variable
 * condition variable. 
 */
pthread_mutex_t threadPoolMutex;


/**
 * Prototype for createInserterThreads
 */
void createInserterThreads();


/**
 * A prototype for adding new records.
 */
void* addNewRecords(void* arg);


/**
 * Prints the hash table
 */
void printHashTable()
{
	/* Go through the hash table */
	for(vector<hashTableCell>::const_iterator hashIt = hashTable.begin(); hashIt != hashTable.end(); ++hashIt)
	{
		fprintf(stderr, "%d-%s-%s\n", hashIt->rec.id, 
		hashIt->rec.firstName.c_str(), 
		hashIt->rec.lastName.c_str());
	}
}

/**
 * Adds a record to the front of the hashtable
 * @param newRec - the record to add
 */
void addToHashTable(const record& newRec, const int& index)
{
	/**
	 * TODO: grab mutex of the hash table cell
	 */
	
	hashTableCell* hashTableCellPtr = &hashTable.at(index);

	hashTableCellPtr->lockCell();
	/* Hash, and save the record */
	hashTable.at(index).rec = newRec;
	
	hashTableCellPtr->unlockCell();
	

	/**
	 * TODO: release mutex of the hashtable cell
	 */
}

/**
 * Swap two records within the hashtable
 */
void swapHashTableRecords(const int& pos_one, const int& pos_two)
{
	if(pos_one != pos_two)
	{
		if ( ( pos_one < 0 || pos_one > NUMBER_OF_HASH_CELLS ) || ( pos_two < 0 || pos_two > NUMBER_OF_HASH_CELLS ) )
		{
			cout << "INVALID SWAP" << endl;
			cout << pos_one << endl;
			cout << pos_two << endl;
		}
		else
		{
			/* Get pointer to the hash table record */
			hashTableCell* hashTableCellOnePtr = &hashTable.at(pos_one); 
			hashTableCell* hashTableCellTwoPtr = &hashTable.at(pos_two); 

			/**
			 * TODO: lock the mutex protecting the hashtable cell (by calling lockCell()). 
			 */
			hashTableCellOnePtr->lockCell();
			hashTableCellTwoPtr->lockCell();

			swap(*hashTableCellOnePtr,*hashTableCellTwoPtr);
			
			/**
			 * TODO: release mutex of the cell. Hint: call unlockCell() to release
			 * mutex protecting the cell.
			 */
			hashTableCellOnePtr->unlockCell();
			hashTableCellTwoPtr->unlockCell();
		}
	}
}



/**
 * Loads the database into the hashtable
 * @param fileName - the file name
 * @return - the number of records left.
 */
int populateHashTable(const string& fileName)
{	
	int count = 0;
	/* The record */
	record rec;

	/* Open the file */
	ifstream dbFile(fileName.c_str());

	/* Is the file open */
	if(!dbFile.is_open())
	{
		fprintf(stderr, "Could not open file %s\n", fileName.c_str());
		exit(-1);
	}


	/* Read the entire file */
	while(!dbFile.eof())
	{
		/* Read the id */
		dbFile >> rec.id;

		/* Make sure we did not hit the EOF */
		if(!dbFile.eof())
		{
			/* Read the first name and last name */
			dbFile >> rec.firstName >> rec.lastName;

			/* Add to hash table */
			addToHashTable(rec, count);	
			
			count++;
		}
	}

	/* Close the file */
	dbFile.close();
}

/**
 * Gets ids to process from work list
 * @return - the id of record to look up, or
 * -1 if there is no work
 */
position getPositionFromLookUp()
{
	/* The position */
	position pos = { -1, -1 };

	/* TODO: Lock the mutex proctecting the idsToLookUpList */
	
	pthread_mutex_lock(&lookUpMutex);

	/* Remove id from the list if exists */
	if(!positionLookUpList.empty()) 
	{ 
		pos = positionLookUpList.front(); 
		positionLookUpList.pop_front(); 
	}
	else
	{
		flag = true;
	}

	pthread_mutex_unlock(&lookUpMutex);
	/* TODO: Release idsToLookUpListMutex  */

	return pos;
}

/**
 * Add id of record to look up
 * @param id - the id to process
 */
void addPositionToLookUp(const position& pos)
{
	/* TODO: Lock the mutex meant to protect idsToLookUpListMutex the list mutex */

	pthread_mutex_lock(&lookUpMutex);

	/* Add the element to look up */
	positionLookUpList.push_back(pos);
	
	pthread_mutex_unlock(&lookUpMutex);

	/* TODO: Release the idsToLookUpList mutex */
}

/**
 * Adds a record to hashtable
 * @param id the id of the record to retrieve
 * @return - the record from hashtable if exists;
 * otherwise returns a record with id field set to -1
 */
record getHashTableRecord(const int& index)
{
	record rec = { -1, "", ""};
	
	if ( ( index < 0 || index > NUMBER_OF_HASH_CELLS ))
	{
		cout << "INVALID RETRIEVE" << endl;
		cout << index << endl;
	}
	else
	{
		hashTableCell* hashTableCellPtr = &hashTable.at(index); 
		hashTableCellPtr->lockCell();
		rec = hashTableCellPtr->rec;
		hashTableCellPtr->unlockCell();
	}
	return rec;
}

/**
 * This is a function in which the worker threads (i.e., the threads
 * responsible for looking up the records requested by the client),
 * spend the entirety of their existance. That is, they sleep on the 
 * condition variable until woken up, and upon awakening start removing
 * the ids of requested records from the idsToLookUp list, looking them up,
 * and replying to the client with the looked up records.      
 * @param thread argument
 */
void* threadPoolFunc(void* arg)
{
	/* Just a variable to store the id of the requested record. */
	position pos; 
	
	/* TODO: Lock the mutex protecting the condition variable on which threads
	* sleep. The name of the mutex depends on what you declared it to be above.
	*/
	pthread_mutex_lock(&threadPoolMutex);

	/* Remove the requsted record id from the idsToLookUp list. */
	pos = getPositionFromLookUp();
	
	while(pos.low != -1 && !flag)
	{			
		if(pos.high - pos.low > 0)
		{
			int pivot = pos.high;
			int leftBoundary = pos.low;
			int rightBoundary = pos.high;
			cout << "-----" << endl;
			cout << "Pivot: " << pivot << " leftBoundary: " << leftBoundary << " rightBoundary: " << rightBoundary << endl;
			
			while(leftBoundary <= rightBoundary)
			{
				
				while( getHashTableRecord(leftBoundary).id < getHashTableRecord(pivot).id )
				{
					leftBoundary++;
					if(leftBoundary == pivot)
					{
						continue;
					}
					//cout << "leftBoundary++ : " << leftBoundary << endl;
				}
				while( getHashTableRecord(rightBoundary).id > getHashTableRecord(pivot).id )
				{
					rightBoundary--;
					if(rightBoundary == pivot)
					{
						continue;
					}
					//cout << "rightBoundary-- : " << rightBoundary << endl;
				}
				if(leftBoundary <= rightBoundary)
				{
					swapHashTableRecords(leftBoundary, rightBoundary);
					cout << "Swapped: " << leftBoundary << " with " << rightBoundary << endl;
					leftBoundary++;
					rightBoundary--;
				}
			}		
			
			pos.low = 0;
			pos.high = rightBoundary;
			addPositionToLookUp(pos);
			cout << "leftBoundary: " << pos.low << " rightBoundary: " << pos.high << endl;
		
			pos.low = leftBoundary;
			pos.high = NUMBER_OF_HASH_CELLS - 1;
			addPositionToLookUp(pos);
			cout << "leftBoundary: " << pos.low << " rightBoundary: " << pos.high << endl;
			
			pthread_cond_wait(&threadPoolCond, &threadPoolMutex);
		}
	}
	
	
	

	/* If getIdsToLookUp() has returned a -1, it means there are no records to 
	 * look up. In this case, the thread should sleep on the condition variable.
	 */

	pthread_mutex_unlock(&threadPoolMutex);
	//cout << "id: " << rec.id << " fName: " << rec.firstName << " lName: " << rec.lastName << endl;
}

/**
 * Wakes up a thread from the thread pool
 */
void wakeUpThread()
{
	/* TODO: Lock the mutex protecting the condition variable from race conditions.
	 * The name of the mutex and the condition variable depend on what you named
	 * them above. First, you will need to lock the mutex protecting the condition
	 * variable, call pthread_cond_signal(<condition variable name>) to wake up
	 * a thread, and then release the mutex.
	 * PLEASE NOTE: the mutex you manipulate here must be the same mutex that is
	 * passed as parameter to pthread_cond_wait() in the threadPoolFunc(). Recall,
	 * this ensures that the sleeping thread does not miss the wake up call.           
	 * The TODO's below break this down furthetr:                             
	 */

	pthread_mutex_lock(&threadPoolMutex);
	pthread_cond_signal(&threadPoolCond);
	pthread_mutex_unlock(&threadPoolMutex);

	/* TODO: Lock the mutex proctecting the condition variable against race conditions */

	/* TODO: Call pthread_cond_signal(<condition variable name>) to wake up
	 * a sleeping thread.
	 */      

	/* TODO: Release the mutex protecting the condition variable from race 
	 * conditions.
	 */
}

/**
 * Creates the threads for looking up ids
 * @param numThreads - the number of threads to create
 */
void createThreads(const int& numThreads)
{
	/** TODO: create numThreads threads that call threadPoolFunc() **/
	pthread_t threads[numThreads];

	for(int threadNum = 0; threadNum < numThreads; ++threadNum)
	{
		/* Create the thread */
		if(pthread_create(&threads[threadNum], NULL, threadPoolFunc, NULL) < 0)
		{
			perror("pthread_create");
			exit(-1);
		}
		//cout << "Thread Created" <<  endl;
	}
}

/**
 * Creates threads that update the database
 * with randomly generated records
 */
void createInserterThreads()
{
	/* TODO: create NUM_INSERTERS threads that spin in a loop and add new elements
	 * to the hashtable by continually calling addNewRecords(). 
	 * 
	 * ADVICE: it is highly recommended that you globally declare a vector of type 
	 * pthread_t (e.g., vector<pthread_t> inserterTids) and in the vector store the
	 * id of each created thread. It will come in handy when the server is terminating
	 * and needs to call pthread_join() for all threads.          
	 */
	pthread_t threads[NUM_INSERTERS];
	

	for(int threadNum = 0; threadNum < NUM_INSERTERS; ++threadNum)
	{
		/* Create the thread */
		if(pthread_create(&threads[threadNum], NULL, addNewRecords, NULL) < 0)
		{
			perror("pthread_create");
			exit(-1);
		}			
	}
}


/**
 * Called by the main thread. In this function, the main thread waits to receive
 * a record request from the client. It then adds the received id request to the
 * idsToLookUp list, and wakes up a worker thread sleeping in the thread pool 
 * (i.e., using pthread_cond_signal()).  
 */
void sortHashTable()
{
	/* The id of the record */
	position pos;

	pos.high = NUMBER_OF_HASH_CELLS - 1;
	pos.low = 0;
	addPositionToLookUp(pos);
	
	/* Wait for messages forever
	 * ADVICE: replace the loop condition below with a boolean variable, which will
	 * be a flag telling all threads, including the main thread, that it is time
	 * to exit. When the user presses Ctrl-C and the cleanUp() function is called,
	 * it will set the flag to true. After the main thread resumes in the loop below,
	 * it will see that it is time to exit, will wake up all the sleeping threads
	 * and will call pthread_join() to wait for all threads to exit.                       
	 */

	while(!flag)
	{
		/* TODO: Wake up a thread to process the newly received id. Recall: we have
		 * a condition variable on which all worker threads sleep. To wake up a worker
		 * thread, we need to signal that condition variable. To achieve all of this,
		 * you need to call wakeUpThread() function, where the appropriate thread wake
		 * up logic is to be implemented.                      
		 */
		wakeUpThread();
	}

	pthread_cond_broadcast(&threadPoolCond);
	
	for(int count = 0; count < inserterTids.size(); count ++)
	{
		pthread_join(inserterTids[count], NULL);
	}
	/* TODO: If we are out of this loop, it means it is time to exit. Call
	 * pthread_cond_broadcast() in order to wake up all threads sleeping on the
	 * condition variable, followed by a loop which calls pthread_join() for all
	 * threads. Where do I get the thread ids to pass to pthread_join()? Well, 
	 * in createThreads() and createInserterThreads() functions you were advised
	 * to globally declare vectors, and in them store the thread ids of the 
	 * corresponding threads. You can use them here.             
	 */      
}

/**
 * Generates a random record
 * @return - a random record
 */
record generateRandomRecord()
{
	/* A record */
	record rec;

	/* Generate a random id */
	rec.id = rand() % NUMBER_OF_HASH_CELLS;	

	/* Add the fake first name and last name */
	rec.firstName = "Random";
	rec.lastName = "Record";

	return rec;
}

/**
 * This is the function executed by the threads that will
 * be inserting new, randomly generate records into the database. 
 * @param arg - some argument (unused)
 */
void* addNewRecords(void* arg)
{	
	/* An instance of the record struct representing
	 * a randomly generated record.
	 */
	pthread_t id;
	id = pthread_self();
	//cout << "Inserter Thread Created id: " << id << endl;
	inserterTids.push_back(id);

	record rec;

	/* 
	 * ADVICE: You may also want to declare a boolean variable e.g., 
	 * inserterTimeToExit. The variable will be a flag representing the moment
	 * when threads should exit. The flag can initially be set to false (meaning
	 * it is not time to exit yet) and be used as a condition for the inserter thread
	 * loop e.g., while (! time to exit) { keep adding records}. When it is time to
	 * exit, the main thread will set the variable to true, which will cause all threads
	 * to break out of the loop.                                   
	 * 
	 * ADVICE: you may want to, at least initially, add "usleep(0.5)" function 
	 * before adding a new record which will somewhat slow down the rate at
	 * which the inserter threads are adding records. Previous experience with this
	 * assignment shows that the inserter threads often add records so fast, they
	 * exhaust the process memory.
	 */                              	

	/* Keep generating random records */	
	while(true)
	{
		/* Generate a random record. */
		rec = generateRandomRecord();
		usleep(0.5);
		/* Add the record to hashtable */
		addToHashTable(rec, rec.id % NUMBER_OF_HASH_CELLS);
	}

}

/**
 * This function defines a signal handler for SIGINT i.e., the
 * signal generated when the user pressed Ctrl-c. Here we want to simply
 * set the globally declared flags meant to tell threads that is is it time
 * to exit. That is, when the user presses Ctrl-c, the main thread will 
 * automatically jump to this function, set all flags indicating that is time 
 * to exit to true, and then resume in processIncomingMessages() where it will
 * wait for the threads to exit.       
 * @param sig - the signal
 */
void tellAllThreadsToExit(int sig)
{
	/* TODO: Set the global variable flags described in threadPoolFunc(), 
	 * addNewRecords(), and processIncomingMessages().
	 */   
	flag = true;
}

int main(int argc, char** argv)
{
	/**
	 * Check the command line
	 */
	if(argc < 3)
	{
		fprintf(stderr, "USAGE: %s <DATABASE FILE NAME> <NUMBER OF THREADS>\n", argv[0]);
		exit(-1);
	}
	pthread_mutex_init(&lookUpMutex, NULL);	
	pthread_mutex_init(&threadPoolMutex, NULL);	
	pthread_cond_init(&threadPoolCond, NULL);

	/* TODO: install a signal handler for the SIGINT signal i.e., the signal
	 * generated when the user presses Ctrl-c to call tellAllThreadsToExit()
	 * function.
	 */ 
	signal(SIGINT, tellAllThreadsToExit); 	

	/* Populate the hash table */
	populateHashTable(argv[1]);

	printHashTable();

	/* Get the number of lookup ID threads */
	numThreads = atoi(argv[2]);


	/* Create the specified number of worker threads. I.e., the threads
	 * that will sleep until the main thread wakes them up (when it recives a record
	 * request)
	 */
	createThreads(numThreads);				

	/* In addition to worker threads, whose job it is to look up the requested 
	 * records, we will also have a group of inserter threads. These threads will
	 * be continuously generating new random records and will be inserting them into
	 * the database.
	 * 
	 * Rationale:
	 * A group of threads inserting records is meant to similate updates to the 
	 * database that involve addition of new records. Many real-world databases
	 * are updated with new data while simultenously being queried for existing 
	 * data. Hence, the setup will give us a real-world glance at the world of 
	 * parallel databases.                             
	 */
	//createInserterThreads();

	/* This function will be invoked by the main thread. In it, it will will 
	 * continously wait to receive a record request from the client (i.e., an id
	 * of the requested record). When it receives the request, it will add the 
	 * received id to the idsToLookUp list, will wake up one of the worker
	 * threads, and will then wait to receive more requests.
	 */
	sortHashTable();
	
	cout  << "----------------" << endl << "Finished Sorting" << endl << "----------------" << endl;
	
	printHashTable();

	return 0;
}
