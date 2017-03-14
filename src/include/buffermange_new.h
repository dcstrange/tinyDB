#ifndef BUFMANAGE
#define BUFMANAGE 1

#include <stdbool.h>
#include <stdio.h>
#include "global.h"
#include "storemanager.h"

#define MAPTB_ITEMNUM 1000000
#define MAX_MAPTBNUM 10

#define MEMBLOCKNUM 1024*256    //this make size of DB buffer 1GB

extern sem_t _Mutex_SB_Access;

typedef int block_type;
enum BlockType {RECBLK,MAPBLK};

typedef struct MemBlkDesp
{
    int despID;
    char* memBlk;
    FILE* fd;
    int phyBlockID;

    bool isUpdated;
    bool isPIN;
    struct MemBlkDesp* nextFree;
    struct MemBlkDesp *warmer,*colder;

    pthread_t tid;
} MemBlkDesp;

/* cache store a record into a block buffer in memory, and return its offset in block.*/
extern int cacheRecord();
//char* blockbuf,int dbaddr,char* recordcontent,int recordsize)
extern char* readblock(int blockid,char* blockbuf);

extern int flushMap();

/**** Interfaces for Meta Manager  ****/
extern void AddThread();
extern void EndThread();
extern int InitDBBuffer();
extern MemBlkDesp* AllocFreeMemBlock();
extern MemBlkDesp* LoadFileBlk(int phyBlkID, FILE* fd);
extern int FlushMemBlk(MemBlkDesp* desp);
extern void HitMemBlock(MemBlkDesp* desp);
extern void PinBlk(MemBlkDesp* desp);
extern void UNPinBlk(MemBlkDesp* desp);
#endif
