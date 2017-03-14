#include "include/buffermange_new.h"
#include <stdlib.h>
/***** Buffer Manager Utilities: Block Allocation,Recycle,Elimination *****/
static char* _DBSpace;
MemBlkDesp *_MemBlkDesp,*_HeadFree,*_TailFree;
static MemBlkDesp *_LRUHot,*_LRUCold;

static int _EliminateBlkNum = 1024*25;  // eliminate 100M once

sem_t _Mutex_SB_Access; // Mutex for a thread accessing shared buffer block
static sem_t _Mutex_SB_Ptr;  // Mutex for a changing the shared buffer ptr( e.g. *_LRUHot,*_LRUCold)

static int _ThreadNum;
static sem_t _Mutex_ThreadNum;

static sem_t _Mutex_Elim;


//void addThread();
//int initDBBuffer();
//char* getMemBlock(int descID);
//int hitMemBlock(MemBlkDesp* blkDesc);
//int flushMemBlk(MemBlkDesp* desp);
//int elimtMemBlocks();
//void freeDBSpace();

/* init whole DB mem Space and it's descriptor */
void _freeDBSpace_()
{
    free(_DBSpace);
}

void _cleanDesp_(MemBlkDesp* desp)
{
    desp->phyBlockID = -1;
    desp->nextFree = NULL;
    desp->warmer = NULL;
    desp->colder = NULL;
    desp->isUpdated = false;
    desp->isPIN = false;
}

/* Get the memory block by it's descriptor ID */
char* _getMemBlock_(int descID)
{
    char* blk = _DBSpace + (descID*BLOCKSIZE);
    return blk;
}


/************************************************
 * Wrappers for eliminate mem blocks functions
 ************************************************/
int _recycMemBlk_(MemBlkDesp* desp)
{
    desp->tid = 0;
    if(_HeadFree == NULL)
    {
        _HeadFree = _TailFree = desp;
        return 0;
    }
    _TailFree->nextFree = desp;
    _TailFree = desp;
    return 0;
}

int _elimtMemBlocks_()
{
    MemBlkDesp* desp;
    int cnt=0;
    while(cnt<_EliminateBlkNum)
    {
        if(_LRUCold == NULL)
            return 0;
        desp = _LRUCold;
        _LRUCold = desp->warmer;

        if(desp->isPIN)
            continue;
        if(desp->isUpdated)
        {
            /*+++++ FLUSH ++++++++*/
            FlushMemBlk(desp);
        }
        _recycMemBlk_(desp);
        cnt++;
    }
    return 0;
}

/**********************************
**** Interfaces for DB Main Process
**********************************/
int InitDBBuffer()
{
    int i;
    //Init memory blocks
    int spacesize = MEMBLOCKNUM*BLOCKSIZE;
    _DBSpace = (char*)calloc(1,spacesize);
    if(_DBSpace == NULL)
        return -1;

    // Init block descriptor
    do
    {
        _MemBlkDesp = (MemBlkDesp*)calloc(MEMBLOCKNUM,sizeof(MemBlkDesp));
    }
    while(_MemBlkDesp==NULL);       /********something wrong with loop********/
    if(_MemBlkDesp == NULL)
        return -1;

    //Init BLock Descriptors
    _HeadFree = _MemBlkDesp;
    MemBlkDesp* blkDesp;
    for(i=0; i<MEMBLOCKNUM; i++)
    {
        blkDesp = _MemBlkDesp+i;
        blkDesp->despID = i;
        blkDesp->memBlk = _getMemBlock_(i);
        blkDesp->nextFree=blkDesp+1;
    }
    blkDesp->nextFree = NULL;
    _TailFree = blkDesp;

    _LRUHot = _LRUCold = NULL;

    Sem_init(&_Mutex_SB_Ptr,0,1);
    Sem_init(&_Mutex_SB_Access,0,0);

    _ThreadNum = 0;
    sem_init(&_Mutex_ThreadNum,0,1);
    sem_init(&_Mutex_Elim,0,1);
    return 0;
}

/*****************************
 *Interfaces for Meta Manager
 *****************************/
void AddThread()
{
    P(&_Mutex_ThreadNum);
    _ThreadNum++;
    V(&_Mutex_SB_Access);
    V(&_Mutex_ThreadNum);
}

void EndThread()
{
    P(&_Mutex_ThreadNum);
    _ThreadNum--;
    P(&_Mutex_SB_Access);
    V(&_Mutex_ThreadNum);
}

MemBlkDesp* AllocFreeMemBlock()
{
    /*** 保证了不存在两个以上的线程同时认为没有空闲块并执行淘汰 ***/
    V(&_Mutex_SB_Access);
    P(&_Mutex_Elim);
    /*THERE IS NO FREE BLOCK, SO DO ELIMINATING*/
    if(_HeadFree == NULL)
    {
        //do eliminating
        P(&_Mutex_ThreadNum);   //avoid there is a new thread which is adding when eliminating.
        int cnt;
        /*+++++++++++ Lock ALL "Access" Operation +++++++++++++*/
        for(cnt=0;cnt<_ThreadNum;cnt++)
        {
            P(&_Mutex_SB_Access);
        }
        /*+++++++++++ Lock ALL "Access" Operation +++++++++++++*/
        _elimtMemBlocks_();
        /*+++++++++++ UnLock ALL "Access" Operation +++++++++++*/
        for(cnt=0;cnt<_ThreadNum;cnt++)
        {
            V(&_Mutex_SB_Access);
        }
        /*+++++++++++ UnLock ALL "Access" Operation +++++++++++*/
        V(&_Mutex_ThreadNum);
    }
    V(&_Mutex_Elim);
    P(&_Mutex_SB_Access);

    P(&_Mutex_SB_Ptr);
    MemBlkDesp* desp = _HeadFree;
    _HeadFree = _HeadFree->nextFree;
    V(&_Mutex_SB_Ptr);

    _cleanDesp_(desp);

    HitMemBlock(desp);
    return desp;
}

MemBlkDesp* LoadFileBlk(int phyBlkID, FILE* fd)
{
    MemBlkDesp* desp = AllocFreeMemBlock();
    if(desp == NULL)    return NULL; /*******ASSERT????*******/
    if(LoadPhyBlk(desp->memBlk,phyBlkID,fd)< 0)    return NULL;
    desp->phyBlockID = phyBlkID;
    desp->fd = fd;
    return desp;
}

/* Flush Memory block */
int FlushMemBlk(MemBlkDesp* desp)
{
    if(desp == NULL || !desp->isUpdated)
        return 0;
    int rnt = WritePhyBlk(desp->memBlk,desp->phyBlockID,desp->fd);
    if(rnt>=0)  desp->isUpdated = false;
    return rnt;
}

void PinBlk(MemBlkDesp* desp)
{
    desp->isPIN = true;
}

void UNPinBlk(MemBlkDesp* desp)
{
    desp->isPIN = false;
    HitMemBlock(desp);
}

void HitMemBlock(MemBlkDesp* blkDesp)
{
    P(&_Mutex_SB_Ptr);
    if(blkDesp->isPIN == true || blkDesp == _LRUHot)      //if the block is PIN, it will not join the eliminated strategy.
    {
        V(&_Mutex_SB_Ptr);
        return;
    }
    if(_LRUCold == NULL)
    {
        _LRUHot = _LRUCold = blkDesp;
        blkDesp->warmer = blkDesp->colder = NULL;
        V(&_Mutex_SB_Ptr);
        return;
    }

    if(blkDesp->colder != NULL)
        blkDesp->colder->warmer = blkDesp->warmer;
    if(blkDesp->warmer != NULL)
        blkDesp->warmer->colder = blkDesp->colder;

    blkDesp->colder = _LRUHot;
    _LRUHot->warmer = blkDesp;
    _LRUHot = blkDesp;
    V(&_Mutex_SB_Ptr);
    return;
}

