#include "include/metaManager.h"

//extern MemBlkDesp* _MemBlkDesp;
/***** Utilities of Record Block *****/
/* load a block with phyBlkID, if it hasn't been cached, loading it from disk, or get it from memory */
MemBlkDesp* _loadRecordBlk_(int blkID,TransTable* trans,FILE* fd)
{
    TransItem* item = (TransItem*)(trans + blkID);
    if(item->desp==NULL)
    {
        MemBlkDesp* desp = LoadFileBlk(blkID,fd);
        item->desp = desp;
    }
    return item->desp;
}


BlkHeader* _getBlkHeader_(MemBlkDesp* desp)
{
    if(desp==NULL)
        return NULL;
    return (BlkHeader*)desp->memBlk;
}
OffsetTable* _getBlkOffTable_(MemBlkDesp* desp)
{
    return (OffsetTable*)(desp->memBlk+sizeof(BlkHeader));
}
void _initRecordBlk_(MemBlkDesp* desp)
{
    BlkHeader* header = _getBlkHeader_(desp);
    header->recordNum = 0;
    header->freeBytes = BLOCKSIZE - sizeof(BlkHeader);
    header->lastOffset = BLOCKSIZE;
}
void _readMemBlock_(char* buf,MemBlkDesp* desp,int offset,size_t n)
{
    memcpy(buf,desp->memBlk+offset,n);
    HitMemBlock(desp);
}
void _writeMemBlock_(char* buf,MemBlkDesp* desp,int offset,size_t n)
{
    memcpy(desp->memBlk+offset,buf,n);
    desp->isUpdated = true;

    HitMemBlock(desp);
}
/* return the record offset in mem block */
int _writeRecord2Blk_(int recordID, char* recBinary,size_t recSize,MemBlkDesp* desp)
{
    BlkHeader* header = _getBlkHeader_(desp);
    OffsetTable* offtb = _getBlkOffTable_(desp);

    OffsetItem* newOffItem = offtb + header->recordNum;
    int newOff = header->lastOffset - recSize;

    newOffItem->recordID = recordID;
    newOffItem->offset = newOff;
    newOffItem->isAvail = 1;
    //write record data binary
    _writeMemBlock_((char*)recBinary,desp,newOff,recSize);
    //write OffsetTable item
    _writeMemBlock_((char*)newOffItem,desp,(int)newOffItem-(int)desp->memBlk,sizeof(OffsetItem));
    header->recordNum++;
    header->lastOffset = newOff;
    header->freeBytes = header->freeBytes-recSize-sizeof(OffsetItem);
    return newOff;
}
int _lookupRecBlkFreeSpace_(MemBlkDesp* desp)
{
    BlkHeader* header = _getBlkHeader_(desp);
    return header->freeBytes;
}


/* The Strategy about HOW TO GET a EXIST BLOCK in DATBASE WHICH HAS SPARE SPACE of "size" FOR NEW RECORD INSERTING.
    THIS WILL BE REWRITED TO BE A BETTER ONE */
MemBlkDesp* _giveSpareBlock4NewRec_(Table* tb,size_t size)
{
    MetaInfo* meta = tb->stt;
    if(meta->maxblkID<0)
        return NULL;
    MemBlkDesp* desp = _loadRecordBlk_(meta->maxblkID,meta->myTrans,tb->dataTable);
    BlkHeader* header = _getBlkHeader_(desp);
    if(size>header->freeBytes)
        return NULL;
    return desp;
}
int _flushWholeTable_(Table* tbmeta,MetaInfo* meta)
{
    TransTable* myTrans = meta->myTrans;
    TransItem* curItem;
    int maxBlkID = meta->maxblkID;
    int i,cnt=0;

    for(i=0; i<=maxBlkID; i++)
    {
        curItem = myTrans + i;
        if(curItem->desp!=NULL && meta->tid==curItem->desp->tid)
            FlushMemBlk(curItem->desp);
        cnt++;
    }
    return cnt;
}


/***************************
*Wrapper for Tables Info
****************************/
static struct tbDesp *FirstTB = NULL;
Table* findTableFlag(char* dbname,char* tbname)
{
    if(FirstTB==NULL)
        return NULL;
    if(strcmp(FirstTB->tb->belongDB,dbname)==0 && strcmp(FirstTB->tb->tb_name,tbname)==0)
        return FirstTB->tb;
    struct tbDesp* tmpDesp = FirstTB;
    while(tmpDesp->next!=NULL)
    {
        if(strcmp(tmpDesp->next->tb->belongDB,dbname) == 0 && strcmp(tmpDesp->next->tb->tb_name,tbname)==0)
        {
            struct tbDesp* newhead = tmpDesp->next;
            tmpDesp->next = tmpDesp->next->next;
            newhead->next = FirstTB;
            FirstTB = newhead;
            return FirstTB->tb;
        }
        else
            tmpDesp = tmpDesp->next;
    }
    return NULL;
}

int addTableFlag(Table* tb)
{
    struct tbDesp* newDesp = (struct tbDesp*)malloc(sizeof(struct tbDesp));
    newDesp->tb = tb;
    newDesp->next = FirstTB;
    FirstTB = newDesp;
    return 0;
}

int _delTableFlag_(char* dbname,char* tbname)
{
    if(FirstTB==NULL)
        return 0;
    if(strcmp(FirstTB->tb->belongDB,dbname)==0 && strcmp(FirstTB->tb->tb_name,tbname)==0)
    {
        FirstTB = FirstTB->next;
        return 0;
    }
    struct tbDesp* tmpDesp = FirstTB;
    while(tmpDesp->next!=NULL)
    {
        if(strcmp(tmpDesp->next->tb->belongDB,dbname) == 0 && strcmp(tmpDesp->next->tb->tb_name,tbname)==0)
        {
            tmpDesp->next = tmpDesp->next->next;
            return 0;
        }
        else
            tmpDesp = tmpDesp->next;
    }
    return 0;
}

/***** Interface for Record Manager(RM) *****/

/* Load the revalent objects when open a table.
    Load the table schema and the Statistic info For Initializing MetaInfo */
Table* OpenTable(char* dbname,char* tbname,char* errmsg)
{
    Table* tb = findTableFlag(dbname,tbname);
    if(tb==NULL)
    {
        FILE* fmeta = tb_des_open(dbname,tbname,"rb+");
        FILE* fdata =tb_datatable_open(dbname,tbname,"rb+");
        if(fmeta == NULL || fdata == NULL)
        {
            sprintf(errmsg,"table <%s> has not been created or the related files have been damaged.\n",tbname);
            return NULL;
        }
        tb = (Table*)malloc(sizeof(Table));
        tb->stt = (MetaInfo*)malloc((sizeof(MetaInfo)));

        tb->belongDB = strdupxy(dbname);
        tb->tb_name = strdupxy(tbname);
        tb->dataTable = fdata;

        /* Load the Schema info ( the 1st block in the meta file) */
        int rn = tb_schema_load(tb,fmeta);

        /* Load lthe Statistic info ( the 2nd block in the meta file) */
        /* Load Map Table */
        int MaxRecNum = 50000000;
        tb->stt->myMap = (MapTable*)calloc(MaxRecNum,sizeof(MapItem));    /**暂时假设Map Tabble最多可映射10M个record**/
        rn = tb_statistic_load(tb->stt,fmeta);

        /* create transform table */
        tb->stt->myTrans = (TransTable*)malloc(1000000*sizeof(TransItem));    /**暂时假设transform table可表示1M个block，即可表示容量最大为1M*4K = 4GB 的Table**/
        addTableFlag(tb);
    }
    return tb;
}

int CloseAllTable()
{
    struct tbDesp *desp = FirstTB;
    while(desp!=NULL)
    {
        CloseTable(desp->tb);
        desp = desp->next;
    }
    return 0;
}

void CloseTable(Table* tb)
{
    P(&_Mutex_SB_Access);
    // flush meta data
    MetaInfo *meta = tb->stt;
    FILE* fmeta = tb_des_open(tb->belongDB,tb->tb_name,"rb+");
    if(tb_schema_flush(tb,fmeta)<0 || tb_statistic_flush(meta,fmeta)<0)
    {
        printf("Error:CANNOT close the Table.\r\n");
        V(&_Mutex_SB_Access);
        return;
    }
    // change the meta file size
    char path[100];
    tb_metafile_path(path,tb->belongDB,tb->tb_name);
    size_t fsize = BLOCKSIZE*2 + (meta->maxRecordID+1)*sizeof(MapItem);
    int tc = truncate(path,fsize);

    // flush record data
    _flushWholeTable_(tb,meta);
    // change the data file size
    tb_datafile_path(path,tb->belongDB,tb->tb_name);
    fsize = BLOCKSIZE * (meta->maxblkID+1);
    tc = truncate(path,fsize);

//////////////    free(meta->myMap);
//////////////    free(meta->myTrans);
//////////////    free(tb);   /***不完整***/
//////////////    free(meta);
    V(&_Mutex_SB_Access);
}

/* insert a new record */

MemBlkDesp* InsertRecBin_MM(char* recBinary,Table* tb,char* errmsg)
{
    P(&_Mutex_SB_Access);
    MetaInfo* meta = tb->stt;
    MapTable* myMap = meta->myMap;
    TransTable* myTrans = meta->myTrans;

    size_t recUsedBytes = tb->recordbinary_length + sizeof(OffsetItem);
    MemBlkDesp* spareDesp = _giveSpareBlock4NewRec_(tb,recUsedBytes);

    /*** Apply for a new mem block. ***/
    //when there is no EXIST block has enough space to insert a new record,
    if(spareDesp == NULL)
    {
        // Allocate a new Block and init it as Reacord Block.
        MemBlkDesp* newDesp = AllocFreeMemBlock();
        if(newDesp==NULL)
        {
            sprintf(errmsg,"Reocord <%s> Insertation Error\n");
            return NULL;
        }
        newDesp->fd = tb->dataTable;
        _initRecordBlk_(newDesp);
        // add a new item to Transform Table.
        meta->maxblkID++;
        TransItem* newTransItem = myTrans + meta->maxblkID;
        newTransItem->phyBlkID = newDesp->phyBlockID = meta->maxblkID;
        newTransItem->desp = newDesp;

        spareDesp = newDesp;
    }
    meta->maxRecordID++;
    meta->recordCnt++;

    //write record Binary into mem block
    off_t offset = _writeRecord2Blk_(meta->maxRecordID,recBinary,tb->recordbinary_length,spareDesp);

    // add a new item to Map table
    MapItem* newMapItem = myMap + meta->maxRecordID;
    newMapItem->recordID = meta->recordCnt;
    newMapItem->phyBlkID = meta->maxblkID;
    newMapItem->offset = offset;
    V(&_Mutex_SB_Access);
    return spareDesp;
}
MemBlkDesp* InsertRecord_MM(char* recordstr,Table* tb,char* errmsg)
{
    char recBinary[4096];
    if(record_string_serialize(recBinary,recordstr,tb,errmsg)<0)
        return NULL;
    return InsertRecBin_MM(recBinary,tb,errmsg);
}

/* lookup the record binary content by given record id, return the binary size */
int LookupRecord_MM(char* buf,int recordID,Table* tb)   /*** 待改进(1.mapitem的定位.2.针对边长记录的优化***/
{
    // lookup the blockid which record belongs from Map.
    MetaInfo* meta = tb->stt;
    MapItem* mapItem = (MapItem*)(meta->myMap+recordID);
    int phyBlkID = mapItem->phyBlkID;
    if(phyBlkID == -1)
        return -2;
    int off = mapItem->offset;
    P(&_Mutex_SB_Access);
    // load the block
    MemBlkDesp* desp = _loadRecordBlk_(phyBlkID,meta->myTrans,tb->dataTable);

    // copy the record content
    memcpy(buf,desp->memBlk+off,tb->recordbinary_length);
    V(&_Mutex_SB_Access);
    return tb->recordbinary_length;
}

int DeleteRecord_MM(int recordID,Table* tb)
{
    MetaInfo* meta = tb->stt;
    MapItem* mapItem = (MapItem*)(meta->myMap+recordID);
    mapItem->phyBlkID = -1;         /****不完善.*****/
    return 0;
}
