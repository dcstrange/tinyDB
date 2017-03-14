#ifndef MM
#define MM
#include "buffermange_new.h"
#include "metaUtility.h"

extern sem_t _Mutex_SB_Access;


/***** Interface for Record Manager(RM) *****/
extern Table* OpenTable(char* dbname,char* tbname,char* errmsg);
extern void CloseTable(Table* tb);
extern int FlushWholeTable(Table* tbmeta);
extern int LookupRecord_MM(char* buf,int recordID,Table* tb);  //lookup the content by given recordID
extern MemBlkDesp* InsertRecord_MM(char* recordstr,Table* tb,char* errmsg);
extern int DeleteRecord_MM(int recordID,Table* tb);


/**** for test...****/
extern BlkHeader* _getBlkHeader_(MemBlkDesp* desp);
extern MemBlkDesp* _loadRecordBlk_(int blkID,TransTable* trans,FILE* fd);


extern int _delTableFlag_(char* dbname,char* tbname);
#endif
