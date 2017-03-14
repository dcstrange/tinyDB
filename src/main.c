#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include "include/index/index_Hash.h"
#include "include/buffermange_new.h"
#include "include/metaManager.h"
#include "include/metaUtility.h"
#include <unistd.h>
#include "include/global.h"
#include "include/recordmanager.h"
#include "y.tab.h"
Table* test_tb_create();
void importDatasTest();
void hashIndexTest(Table* tb);
extern sem_t _Mutex_SB_Access;

int main()
{
    InitDBBuffer();
    AddThread();

    printf("-------------Terminal to TinyDB!-------------\n");
    int rnt;
    while(1)
    {
        parser_init();
        rnt = yyparse();
        if(rnt==-1)
            break;
    }
    printf("-------------see u again!-------------\n");

/************************************/
////////    Table tb;
////////    tb.tb_name = "TB_TEST";
////////    tb.belongDB = "newdb";
////////    MetaInfo meta;
////////
////////    InitDBBuffer();
////////    AddThread();
////////    OpenTable(&tb,&meta);
////////    hashIndexTest(&tb,&meta);
////////    CloseTable(&tb,&meta);
/************************************/

//    Table* tb = test_tb_create();
//    MetaInfo* meta = (MetaInfo*)malloc(sizeof(MetaInfo));
////    Table* tb = (Table*)malloc(sizeof(Table));
//    tb_create(tb);
//
//    InitDBBuffer();
//
//    //MetaInfo meta;
////    tb->tb_name = "TB_TEST";
////    tb->belongDB = "newdb";
//    AddThread();
//    clock_t start,finish;
//    double duration;
//    start = clock();
//    importDatasTest(tb,meta);
//    finish = clock();
//    duration=(double)(finish-start)/CLOCKS_PER_SEC;
//    printf("%f 秒\n",duration);
//
//    start = clock();
//    CloseTable(tb);
//    finish = clock();
//    duration=(double)(finish-start)/CLOCKS_PER_SEC;
//    printf("%f 秒\n",duration);
//
//    printf("You have done the Data Importing!\n");
//    /* ++++++++++++ Import Data Done~++++++++++ */
//
//    printf("Do you want to see?(Y/N) :");
//    char input = getchar();
//    if(input != 'Y') return;
//    tb = (Table*)malloc(sizeof(Table));
//    tb->tb_name = "TB_TEST";
//    tb->belongDB = "newdb";
//    char* recBin = (char*)malloc(100);
//    char* recStr = (char*)malloc(100);
//
//    OpenTable("newdb","TB_TEST",NULL);
//    LookupRecord_MM(recBin,1,tb);
//    record_binary_deserialize(recStr,recBin,tb);
//
//    printf("%s\n",recStr);
//    LookupRecord_MM(recBin,2,tb);
//    record_binary_deserialize(recStr,recBin,tb);
//    printf("%s\n",recStr);
//    CloseTable(tb);
    return 0;
}

void importDatasTest(Table* tb,MetaInfo* meta)
{
    char* dataFile = "/home/ly/test_num_big";
    FILE* fd = fopen(dataFile,"rt");

    if(fd == NULL)
    {
        printf("Open ImportFile Failure\r\n");
        return;
    }

    int i,cnt=0;
    char* line = (char*)malloc(4096);
    char* read;
    char* tmp;
    MemBlkDesp* oldDesp = NULL;
    MemBlkDesp* newDesp = NULL;
    while(1)
    {
        read = fgets(line,4096,fd);
        if(read == NULL)
            break;
        i = strlen(line)-1;
        tmp = line + i;
        while(*tmp == '\r' || *tmp == '\n')
        {
            *tmp = 0;
            tmp--;
        }
        char errmsg[100];
        newDesp = InsertRecord_MM(line,tb,errmsg);

        /* ++++++++++PRINT++++++++++++++*/
        if(newDesp!= oldDesp && oldDesp!=NULL)
        {
            printf("The Allocated Block: ID = %d,\tinclude %d records,\tremains %d bytes Free Space\r\n",oldDesp->phyBlockID,_getBlkHeader_(oldDesp->memBlk)->recordNum,_lookupRecBlkFreeSpace_(oldDesp));
        }
        oldDesp = newDesp;
        /*++++++++++++++++++++++++++++++++*/
        cnt++;
    }
    printf("Imported %d records\n",cnt);
    if(ferror(fd)!=0)
        printf("Load import file failure.\n");
}

void hashIndexTest(Table* tb)
{
    MetaInfo* meta = tb->stt;
    Hashbucket* myBuckets = init_hash(NUMERIC);
    int i;
    char buf[200];
    int curPro=0;
    printf("开始创建索引...\n");
    for(i = 0;i<=meta->maxRecordID;i++)
    {
        LookupRecord_MM(buf,i,tb);
        int* stdNo = (int*)malloc(sizeof(int));
        memcpy(stdNo,buf,sizeof(int));
        inserthashitem(stdNo,i,myBuckets);
        if((int)((float)i*100/meta->maxRecordID) >= curPro)
        {
            printf("%d\%\n",curPro);
            curPro+=10;
        }
    }
    //---------------------------------------------
    printf("索引创建完成!\n");
    printf("输入学生学号:\n");

    read(STDIN_FILENO,buf,10);
    int no = atoi(buf);
    int id = gethashitem(&no,myBuckets);

    char binary[200];
    LookupRecord_MM(binary,id,tb);
    record_binary_deserialize(buf,binary,tb);
    printf("%s",buf);

}

Table* test_tb_create()
{
    Table* tb = (Table*)malloc(sizeof(Table));
    tb->tb_name = "TB_TEST";
    tb->belongDB = "newdb";
    tb->attr_num = 3;

//    char** attrnames = (char**)malloc(3*sizeof(char*));
    char* name1 = (char*)malloc(20);
    char* name2 = (char*)malloc(20);
    char* name3 = (char*)malloc(20);
    sprintf(name1,"%s","STUDENT_NO");
    sprintf(name2,"%s","STUDENT_NAME");
    sprintf(name3,"%s","SCORE");
//    attrnames[0]=name1;
//    attrnames[1]=name2;
//    attrnames[2]=name3;
//    tb->attr_names = attrnames;

    attrinfo* attrs = (attrinfo*)malloc(tb->attr_num*sizeof(attrinfo));
    attrs[0].name = name1;
    attrs[1].name = name2;
    attrs[2].name = name3;
    attrs[0].type = INT;
    attrs[1].type = CHAR;
    attrs[2].type = INT;

    attrs[0].size_used = 1;
    attrs[1].size_used = 10;
    attrs[2].size_used = 1;

    tb->attrs = attrs;

//    int* attrtypes =(int*)malloc(3*sizeof(int));
//    attrtypes[0] = INT;
//    attrtypes[1] = CHAR;
//    attrtypes[2] = INT;
//    tb->attr_types = attrtypes;
//
//    int* attrtype_use =(int*)malloc(3*sizeof(int));
//    attrtype_use[0] = 1;
//    attrtype_use[1] = 10;
//    attrtype_use[2] = 1;
//    tb->attrsize_used = attrtype_use;

    tb->index_num = 2;
    int* index_attr = (int*)malloc(2*sizeof(int));
    index_attr[0] = 0;
    index_attr[1] = 1;
    tb->index_attr = index_attr;

    int* index_type = (int*)malloc(2*sizeof(int));
    index_type[0] = SEQ;
    index_type[1] = HASH;
    tb->index_type = index_type;

    tb->primary_num =2;
    int* prmkeys = (int*)malloc(2*sizeof(int));
    prmkeys[0] = 0;
    prmkeys[1] = 1;
    tb->prmkeys = prmkeys;
    return tb;
}

void test_tb_info_format2string()
{
    char linebuf[_MAXLEN_META_TXTLINE+1];
    Table* tb = test_tb_create();
    tb_des_serialize(linebuf,tb);
    printf("%s",linebuf);
}
