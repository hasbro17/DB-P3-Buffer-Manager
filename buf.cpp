/* Name: Haseeb Tariq
*  Student ID: 9071761317
   Net ID: htariq

Summary: The buffer manager manages the state of the buffer pool frames by allocating and reading/writing pages into the frames.
It keeps track of the frame states with the help of the BufHashTable and BufDesc classes.
*/
#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

/*
*This is the class constructor. Allocates an array for the buffer pool with bufs page frames and a corresponding BufDesc table. 
*The way things are set up all frames will be in the clear state when the buffer pool is allocated. The hash table will also start out in an empty state. We have provided the constructor.
*/
BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

/*
*Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
*/
BufMgr::~BufMgr() {
	int pageNo;
	Page *pagePtr;
	File *file;
	//Flush out the dirty pages
	for(int i=0; i<numBufs; i++)
	{
		//flush out dirty pages first
		if(bufTable[i].dirty)
		{
			//get the page number, page and file pointers
			pageNo=bufTable[i].pageNo;
			file=bufTable[i].file;
			pagePtr=&bufPool[i];

			//write that page to disk
			file->writePage(pageNo, pagePtr);
		}

	}
	//dealloc the buffer pool and descriptor table
	delete[] bufPool;
	delete[] bufTable;

}

/*
*Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. 
*Updates the hashtable accordingly when it kicks out a page.
@param: frame-> used to return the address of the frame just freed.
@return: Status -> OK or error
*/
const Status BufMgr::allocBuf(int & frame) {
	int i;
	File *file;
	Status retval;
	bool allPinned=true;

	for(int i=0; i< numBufs; i++)
	{
		if( (bufTable[i].valid && bufTable[i].pinCnt==0) || (!bufTable[i].valid) )//if any valid page is unpinned or there is an invalid page ready to be kicked out
		{
			allPinned=false;
			break;
		}
	}
	
	//if all pages are valid and pinned
	if(allPinned)
		return BUFFEREXCEEDED;

	while(1)
	{
		advanceClock();
		i=clockHand;
		if(!bufTable[i].valid)//invalid so just allocate the frame
		{
			frame=i;
			return OK;
		}
		else//valid page
		{
			if(bufTable[i].refbit)
			{
				bufTable[i].refbit=false;
				continue;
			}

			if(bufTable[i].pinCnt>0)
				continue;

			file=bufTable[i].file;
			if(bufTable[i].dirty)//flush to disk if dirty
			{
				retval=file->writePage(bufTable[i].pageNo,&bufPool[i]);//flush out to disk
				if(retval!=OK)//return UNIXERR if problem at the IO layer
					return UNIXERR;
			}

			hashTable->remove(file,bufTable[i].pageNo);//remove entry from hashTable
			//set the frame number to the free frame
			frame=i;
			return OK;
		}
	}

}

/*
*Checks whether a page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number.
*@param: the file and page number to be read. And the page stores the page just read
@return: Status OK if succeded, Error otherwise 
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
	int frameNo;
	Status retval;
	//Check Hashtable for the file and pageNo
	if( (hashTable->lookup(file, PageNo, frameNo)) !=OK)//page not found in bufPool
	{	
		//allocate a new frame
		if( (retval=allocBuf(frameNo)) != OK)
			return retval;//either BUFFEREXCEEDED or UNIXERR from the allocBuff

		//read the page from disk into the newly allocated frame location
		if( (retval=file->readPage(PageNo, &bufPool[frameNo]) ) != OK)
			return UNIXERR;//even though it could be either BADPGPTR or BADPGNO

		//Insert the pageNumber and file into the hash table
		if( (retval=hashTable->insert(file, PageNo, frameNo)) !=OK)
			return retval;//return if HASHTBLERROR occurs

		//Set up the frame properly now
		bufTable[frameNo].Set(file, PageNo);
		//Set the pointer to the page to return
		page=&bufPool[frameNo];
		return OK;
	}
	else//Page found in buffer pool
	{
		//set refBit and increment pinCnt
		bufTable[frameNo].refbit=true;
		bufTable[frameNo].pinCnt++;
		page=&bufPool[frameNo];
		return OK;
	}
}

/*
*Unpins the page number specified by the fila and PageNo. Sets the dirty bit to true if page was modified
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
	int frameNo;
	Status retval;
	//See if page in buffer pool
	if( (retval=hashTable->lookup(file, PageNo, frameNo)) != OK)
		return retval;//HASHNOTFOUND
	
	//Check if page pinned
	if( bufTable[frameNo].pinCnt==0)
		return PAGENOTPINNED;

	bufTable[frameNo].pinCnt--;//decrement pinCnt
	if(dirty)
		bufTable[frameNo].dirty=true;
	return OK;
}

/*
*Allocates a page in the buffer pool by first allocating a new page in the file and setting up a free frame with that page number
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
	// TODO: Implement this method by looking at the description in the writeup.
	Status retval;
	int frameNo;
	//allocate a new page in disk for that file and give back pageNo
	if( (retval=file->allocatePage(pageNo)) != OK)
		return retval;//UNIXERR
	
	//allocate a frame in the bufPool
	if( (retval=allocBuf(frameNo)) != OK)
		return retval;//UNIXERR or BUFFEREXCEEDED
	
	//Insert the pageNumber and file into the hash table
	if( (retval=hashTable->insert(file, pageNo, frameNo) ) !=OK)
		return retval;//return if HASHTBLERROR occurs (duplicate or malloc fail)

	//Set up the frame
	bufTable[frameNo].Set(file, pageNo);

	//set the return value for the page pointer. frameNo already set
	page=&bufPool[frameNo];

	return OK;
}

/*
*Removes the given pageNo from the file and if found then also from the bufferPool
*/
const Status BufMgr::disposePage(File* file, const int pageNo) {
	int frameNo;
	//See if page in buffer pool
	if(hashTable->lookup(file, pageNo, frameNo) == OK)
	{
		//clear the page
		bufTable[frameNo].Clear();
		//remove entry from hashtable
		hashTable->remove(file, pageNo);
	}
	//remove page from file
	return file->disposePage(pageNo);
}

/*
*Scan all frame for pages of the particular file and removes them from the frames. Flushes them out to disk first if dirty
*/
const Status BufMgr::flushFile(const File* file) {
	//scan for all pages of the file in the bufPool
	Status retval;
	//for each frame in bufPool
	for(int i=0; i<numBufs; i++)
	{
		if(bufTable[i].file==file)//if page from the file
		{
			if(bufTable[i].pinCnt>0)//if pinned then fail
				return PAGEPINNED;

			//flush to disk if dirty
			if(bufTable[i].dirty)
			{
				retval=bufTable[i].file->writePage(bufTable[i].pageNo,&bufPool[i]);//flush out to disk
				if(retval!=OK)//return UNIXERR if problem at the IO layer
					return retval;
				bufTable[i].dirty=false;
			}

			//remove page entry from hashtable and clear frame
			hashTable->remove(file,bufTable[i].pageNo);//Don't return HASHTBLERR if entry not present
			bufTable[i].Clear();
		}					
	}
		
	return OK;
}

/*
*Prints out the content of each frame in the buffer Pool along with if its pinned or not
*/
void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


