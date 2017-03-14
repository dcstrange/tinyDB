#ifndef _TB_UTIL
#define _TB_UTIL

#include <string.h>
#include "global.h"
#include <stdio.h>
#include "buffermange_new.h"
/* Attribute of a Database */
typedef struct
{
    char* dbname;
} db_attr;

typedef struct
{
    char* name;  /* sequenced namelist of record's attributes */
    unsigned int type;   /* sequenced typelist of record's attributes */
    unsigned int size_used;  /* every item declare the count of the typesize used */
} attrinfo;

/* The basic attribute of a DB table*/

typedef struct
{
    int recordID;
    int phyBlkID;
    int offset;
} MapTable,MapItem;

typedef struct
{
    int phyBlkID;
    MemBlkDesp* desp;
} TransTable,TransItem;

typedef struct
{
    MapTable* myMap;
    TransTable * myTrans;
    int recordCnt, maxRecordID, maxblkID;
    pthread_t tid;
} MetaInfo;

typedef struct
{
    char* belongDB;     /* the name of DB that table belongs to */
    char* tb_name;    /* table name*/

    unsigned int attr_num;
    attrinfo* attrs;	/* the attributes information */

    unsigned int primary_num;
    unsigned int* prmkeys;

    unsigned int index_num;
    unsigned int* index_attr;     //Array of AttrType
    unsigned int* index_type;    //Array of IndexType

    unsigned int recordbinary_length;

    MetaInfo* stt;
    FILE* dataTable;

} Table;

struct tbDesp
{
    Table* tb;
    struct tbDesp* next;
};
typedef struct
{
    int recordCnt,maxRecordID,maxblkID;
} TableRecordStt;

enum RecordOP {OP_I,OP_R,OP_M,OP_S};
enum AttrType
{
    INT,SHORT,LONG,FLOAT,DOUBLE,CHAR,NCHAR,BOOL,DATETIME
};

enum IndexType
{
    SEQ,HASH
};
/**********************
*Wrapper
***********************/

typedef struct
{
    int recordNum;
    int freeBytes;
    int lastOffset;
} BlkHeader;
typedef struct
{
    int isAvail;
    int recordID;
    int offset;
} OffsetTable,OffsetItem;




/***********************/
extern int record_binary_attr_locate(char* attrname,Table* tb,int* attrsize,int* attrType);
extern void db_attr_init(db_attr *dbinfo,char *name);
extern int db_create(db_attr *dbinfo);

enum tb_meta_form
{
    /* The meta file of a table is in the following form.
    >>>>
        flag |
        table_name |
        [(attr_name,attr_type),...] |
        primarykey_idx[] |
        [(index_attr,index_type),...] <<<<
       For the fast to get the position of the item in this form, we define a enum following.
    */
    DES_FLAG,DES_TBNAME,DES_TBATTRINFO,DES_PKEYS,DES_IDXINFO
};

extern void tb_parser(char* sql, Table* tb);
extern int tb_des_insert(Table* tb);
extern int tb_des_update(Table* tb);
extern FILE* tb_des_open(char* dbname,char* tbname,const char* modes);

/***** Interface for RM,MM *****/
extern int record_string_serialize(char* buf,char* recordstr,Table* tbmeta,char* errmsg);
extern int record_binary_deserialize(char* buf,char* recordbinary,Table* tbmeta);
extern int tb_schema_flush(Table* table,FILE* fmeta);
extern int tb_statistic_flush(MetaInfo* meta,FILE* fmeta);
extern FILE* tb_des_open(char* dbname,char* tbname,const char* modes);
extern FILE* tb_datatable_open(char* dbname,char* tbname,const char* modes);

/**********************************/
extern int CreateTable(char* tbname,int attrnum,attrinfo* attrs,char* errmsg);
extern int DropTable(char* tbname,char* errmsg);
extern int CloseAllTable();
extern int attrvalue_serialize_by_type(char* buf,char* str,int type,unsigned int num);

#endif // _TB_UTIL


