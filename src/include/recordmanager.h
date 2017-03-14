#ifndef RM
#define RM

#include "metaUtility.h"
#include "metaManager.h"
/****************************************
** MetaInfo of every sub-SELECT sentence
*****************************************/
typedef struct ColumnList
{
    char* belongTable;
    char* columnName;
    int colOff;
    int colsize;
    int colType;
    struct ColumnList* next;
}ColumnList;

typedef struct
{
    char* tbname;
    char* nickname;
    struct FromList* next;
}FromList;

typedef struct Condition
{
    int op;
    char* tbname;
    int tbID;
    char* columnName;
    int colOff;
    char* data;
    int dataType;
    struct Condition* next;
}Condition;

typedef struct Joins
{
    int op;
    char* tbname1;
    char* tbname2;
    char* col1;
    char* col2;
    struct Joins* next;

    //need to be analyse
    int coltype1;
    int coltype2;
    int off_col1;
    int off_col2;
    int colsize1;
    int colsize2;
}Joins;

typedef struct SubSelection
{
    int isUnique;
    int isAllColumn;
    ColumnList* columns;
    FromList* froms;
    Condition* conds;
    struct Joins* joins;
    struct SubSelection* parent;
}SubSelection;

enum OP{EQ,GE,LE,GT,LT,NE};

/*********************************
*** Objects for Selection Excution
**********************************/
typedef struct StreamDataItem
{
    char* data;
    struct StreamDataItem* next;
}StreamDataItem;

enum Operator{SELOP_PRJ,SELOP_FLT,SELOP_JOIN};
typedef struct ExecRule
{
    int oprater;
    void* relatedStruct;
    void* (*ExecFuntion)(void*);

    int flag;
    int value;
    struct ExecRule* nextExec;
}ExecRule;


extern int InsertRecord(char* dbname, char* tbname,char* recordbin,char* errmsg);

typedef struct tbnameStack
{
    char* tbname;
    struct tbnameStack* next;
}tbnameStackTop;

extern int DeleteRecord(char* tbname,Condition* cond,char* errmsg);
extern  char *strdupxy(char *s);
#endif
