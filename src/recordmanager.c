#include "include/recordmanager.h"
#include "include/operator.h"
#include "include/spj.h"
#include <stdlib.h>
static int _CurRecID;
ExecRule* _genExecRules(SubSelection* subsel,char* outputTBName,char* errmsg);


int InsertRecord(char* dbname, char* tbname,char* recordstr,char* errmsg)
{
    Table* tb = OpenTable(dbname,tbname,errmsg);
    if(tb==NULL)
        return -1;

    if(InsertRecord_MM(recordstr,tb,errmsg)==NULL)
        return -2;
    return 0;
}


int DeleteRecord(char* tbname,Condition* conds,char* errmsg)
{
    Table* tb = OpenTable("newdb",tbname,errmsg);
    if(tb==NULL)    return -1;

    Condition* cond = conds;
    while(cond!=NULL)
    {
        cond->tbID=1;
        cond = cond->next;
    }
    char buf[4096];
    int id;
    for(id=0; id<=tb->stt->maxRecordID; id++)
    {
        LookupRecord_MM(buf,id,tb);
        if(isMatchConds(buf,conds,tb,NULL) == 1)
            DeleteRecord_MM(id,tb);
    }
    return 0;
}


int tmpid = 0;
Table* tmptbs[50];

int SelectRecord(SubSelection* subsel,char* tmptbname,char* errmsg)
{
    //PrintSubSelectionInfo(subsel);

    /************************************
    ** FRAMEWORK FOR SELECTION EXCUTION**
    *************************************/


    /*******************************************************
    *** 1. GENERATING COMPLETED SUB-SELECTION CONFIG *******
    ********************************************************/
    ColumnList* col = subsel->columns;
    while(col!=NULL)
    {
        if(col->belongTable==NULL)
        {
            col->belongTable = subsel->froms->tbname;
        }
        Table* tb = OpenTable("newdb",col->belongTable,errmsg);
        col->colOff = record_binary_attr_locate(col->columnName,tb,&col->colsize,&col->colType);
        col = col->next;
    }
    Table* tb1=NULL;
    Table* tb2=NULL;
    Joins* join = subsel->joins;    ///(目前只支持一个subselection中仅有一次join算子)
    if(join!=NULL)
    {
        FromList* from = subsel->froms;
        while(from!=NULL)
        {
            if(strcmp(from->nickname,join->tbname1)==0)
                join->tbname1 = from->tbname;
            if(strcmp(from->nickname,join->tbname2)==0)
                join->tbname2 = from->tbname;
            from=from->next;
        }
        tb1 = OpenTable("newdb",join->tbname1,errmsg);
        tb2 = OpenTable("newdb",join->tbname2,errmsg);

        join->off_col1 = record_binary_attr_locate(join->col1,tb1,&join->colsize1,&join->coltype1);
        join->off_col2 = record_binary_attr_locate(join->col2,tb2,&join->colsize2,&join->coltype2);
    }
    else
    {
        tb1 = OpenTable("newdb",subsel->froms->tbname,errmsg);
    }

    Condition* cond = subsel->conds;
    while(cond!=NULL)
    {
        if(join==NULL&&cond->tbname==NULL)
        {
            cond->tbname = subsel->froms->tbname;
        }
        if(strcmp(cond->tbname,tb1->tb_name)==0)
        {
            cond->tbID=1;
            cond->colOff = record_binary_attr_locate(cond->columnName,tb1,NULL,NULL);
        }
        else
        {
            cond->tbID=2;
            cond->colOff = record_binary_attr_locate(cond->columnName,tb2,NULL,NULL);
        }
        cond=cond->next;
    }

    /********************************************************
    ***************2. VALIDATION CHECK **********************
    *********************************************************/

    if(join!=NULL)
    {
        if(join->coltype1!=join->coltype2)
        {
            sprintf(errmsg,"%s","The type is wrong.");
            return -1;
        }
        if(join->colsize1!=join->colsize2)
        {
            sprintf(errmsg,"%s","The column size is wrong.");
            return -1;
        }
    }
    /*******************************************************
    *************3. CREATE OUTPUT TABLE ********************
    *******************************************************/
    int colcnt=0;

    if(subsel->isAllColumn)
    {
        colcnt += tb1->attr_num;
        if(join!=NULL)
            colcnt += tb2->attr_num;
    }
    else
    {
        ColumnList* col = subsel->columns;
        while(col!=NULL)
        {
            colcnt++;
            col = col->next;
        }
    }

    attrinfo attrs[colcnt];// = (attrinfo*)malloc(colcnt*sizeof(attrinfo));
    int offsetList[colcnt];
    if(subsel->isAllColumn)
    {
        int i,j;
        for(i=0; i<tb1->attr_num; i++)
        {
            attrs[i].name = tb1->attrs[i].name;
            attrs[i].size_used  = tb1->attrs[i].size_used;
            attrs[i].type = tb1->attrs[i].type;
        }
        if(join!=NULL)
        {
            for(j=i; j<colcnt; j++)
            {
                attrs[j].name = tb2->attrs[j-i].name;
                attrs[j].size_used  = tb1->attrs[j-i].size_used;
                attrs[j].type = tb1->attrs[j-i].type;
            }
        }
    }
    else
    {
        ColumnList* col = subsel->columns;
        int i;
        for(i=0; i<colcnt; i++)
        {
            attrs[i].name = col->columnName;
            attrs[i].size_used = col->colsize;  //wrong
            attrs[i].type = col->colType;

            char* tbname = col->belongTable;
            if(strcmp(tbname,tb1->tb_name)==0)
            {
                offsetList[i] = col->colOff;
            }
            else if(strcmp(tbname,tb2->tb_name)==0)
            {
                offsetList[i] = tb1->recordbinary_length+col->colOff;
            }
            else
            {
                /* ERROR */

            }
            col=col->next;
        }
    }
    CreateTable(tmptbname,colcnt,attrs,errmsg);
    /*******************************************************
    ************4. Generating Execution RULES **************
    *******************************************************/
    ExecRule* rules = _genExecRules(subsel,tmptbname,errmsg);

    /*******************************************************
    ********************5. Executing  **********************
    *******************************************************/
    Table* outputTB = OpenTable("newdb",tmptbname,errmsg);

    initExec();
    _execSelection(rules,outputTB,offsetList,colcnt,tb1,tb2);

    tmptbs[tmpid++] = outputTB;
    if(subsel->parent==NULL)
    {
        char* buf[4096];
        char recstr[4096];
        int id;
        for(id=0; id<=outputTB->stt->maxRecordID; id++)
        {
            if(LookupRecord_MM(buf,id,outputTB)<0)
                continue;
            record_binary_deserialize(recstr,buf,outputTB);
            printf("%s\n",recstr);
        }

        int k;
        for(k = 0; k<tmpid; k++)
        {
            DropTable(tmptbs[k]->tb_name,errmsg);
        }
        tmpid = 0;
    }
    return 0;
    //    Table* tb = OpenTable("newdb",tbname,errmsg);
//    if(tb==NULL)
//        return -1;
//    char buf[4096];
//    char recstr[4096];
//    int id;
//
//    for(id=0; id<=tb->stt->maxRecordID; id++)
//    {
//        if(LookupRecord_MM(buf,id,tb)<0)
//            continue;
//        if(isMatchConds(buf,condcnt,cond,tb) == 1)
//        {
//            record_binary_deserialize(recstr,buf,tb);
//            printf("%s\n",recstr);
//        }
//    }
}



ExecRule* _genExecRules(SubSelection* subsel,char* outputTBName,char* errmsg)
{
    /*
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
    */
    ExecRule* rulefirst=NULL;
    ExecRule* rulelast=NULL;

    if(1)
    {
        ExecRule* rule = (ExecRule*)malloc(sizeof(ExecRule));
        rule->oprater = SELOP_PRJ;
        rule->flag = subsel->isAllColumn;
        rule->relatedStruct = (void*)subsel->columns;
        rule->nextExec = NULL;
        if(rulelast==NULL)
        {
            rulefirst = rulelast = rule;
        }
        else
        {
            rulelast->nextExec = rule;
            rulelast = rule;
        }
    }
    if(subsel->conds!=NULL)
    {
        ExecRule* rule = (ExecRule*)malloc(sizeof(ExecRule));
        rule->oprater = SELOP_FLT;
        rule->relatedStruct = (void*)subsel->conds;
        rule->nextExec = NULL;
        if(rulelast==NULL)
        {
            rulefirst = rulelast = rule;
        }
        else
        {
            rulelast->nextExec = rule;
            rulelast = rule;
        }
    }
    if(subsel->joins!=NULL)
    {
        ExecRule* rule = (ExecRule*)malloc(sizeof(ExecRule));
        rule->oprater = SELOP_JOIN;
        rule->relatedStruct = (void*)subsel->joins;
        rule->ExecFuntion = Join_nestloop;  //method in SPJ
        rule->nextExec = NULL;
        if(rulelast==NULL)
        {
            rulefirst = rulelast = rule;
        }
        else
        {
            rulelast->nextExec = rule;
            rulelast = rule;
        }
    }
    return rulefirst;
}



/********************
** Excution Strategy
**********************/
char* _getnext_PRJ(ExecRule* rule,Table* outputTB,int* offsetList,int colcnt,Table* tb1, Table* tb2);
char* _getnext_FILTER(ExecRule* rule,Table* tb1,Table* tb2);
char* _getnext_JOIN(ExecRule* rule);

int _recProjection(char* dest,char* rec,ColumnList* columns,int* offsetList)
{
    int curPos = 0;
    int i = 0;
    ColumnList* col = columns;
    while(col!=NULL)
    {
        memcpy(dest+curPos,rec+offsetList[i],col->colsize);
        curPos+=col->colsize;
        col = col->next;
        i++;
    }
}

char* _getnext_PRJ(ExecRule* rule,Table* outputTB,int* offsetList,int colcnt,Table* tb1, Table* tb2)
{
    char* recdata;
    char* buf;
    if(rule->nextExec == NULL)
    {
        //Other Way
        int id;
        char* buf = (char*)malloc(tb1->recordbinary_length);
        for(id=0; id<=tb1->stt->maxRecordID; id++)
        {
            if(LookupRecord_MM(buf,id,tb1)<0)
                continue;
            InsertRecBin_MM(buf,outputTB,NULL);
        }
        return NULL;
    }
    else
    {
        int nextOP = rule->nextExec->oprater;

        if(nextOP == SELOP_FLT)
        {
            recdata = _getnext_FILTER(rule->nextExec,tb1,tb2);
        }
        else if(nextOP == SELOP_JOIN)
        {
            recdata = _getnext_JOIN(rule->nextExec);
        }
    }
    if(recdata==NULL)
        return NULL;
    // Projection
    if(rule->flag)
    {
        //isALLColumns
        return recdata;
    }
    else
    {
        buf =  (char*)malloc(outputTB->recordbinary_length);
        _recProjection(buf,recdata,(ColumnList*)rule->relatedStruct,offsetList);
        free(recdata);
        return buf;
    }
}

static int _CurRecID;
char* _getnext_FILTER(ExecRule* rule,Table* tb1,Table* tb2)
{
    char* recdata;
    if(rule->nextExec == NULL)
    {
        //Other Way
        char* buf = (char*)malloc(tb1->recordbinary_length);
        while(_CurRecID <= tb1->stt->maxRecordID)
        {
            if(LookupRecord_MM(buf,_CurRecID++,tb1)<0)
                continue;
            if(isMatchConds(buf,(Condition*)rule->relatedStruct,tb1,tb2))
            {
                return buf;
            }
        }
        return NULL;
    }
    else if(rule->nextExec->oprater == SELOP_JOIN)
    {
        while(1)
        {
            recdata = _getnext_JOIN(rule->nextExec);
            if(recdata==NULL)
                return NULL;
            if(isMatchConds(recdata,(Condition*)rule->relatedStruct,tb1,tb2))
            {
                return recdata;
            }
        }
    }

}

char* _getnext_JOIN(ExecRule* rule)
{
    char* recdata = GetNext_Join(rule->ExecFuntion,(Joins*)rule->relatedStruct);
    return recdata;
}


int _execSelection(ExecRule* rules,Table* outputTB,int* offsetList,int colcnt,Table* tb1,Table* tb2)
{
    char* recdata;
    while(1)
    {
        recdata = _getnext_PRJ(rules,outputTB,offsetList,colcnt,tb1,tb2);
        if(recdata==NULL)
            break;
        InsertRecBin_MM(recdata,outputTB,NULL);
    }
    return 0;
}

void initExec()
{
    _CurRecID = 0;
}

/**********************************
** FUNC FOR OUTPUT YACC PROCESSING*
***********************************/
void printtab(char* str)
{
    if(str==NULL)
    {
        printf("%s\t\t",str);
        return;
    }
    int len = strlen(str);
    if(len<8)
    {
        printf("%s\t\t",str);
    }
    else if(len<16)
        printf("%s\t",str);
    else
    {
        char out[16+1];
        memcpy(out,str,13);
        out[13]='.';
        out[14]='.';
        out[15]='.';
        out[16]='\0';
        printf("%s",out);
    }
}

void printhead(char* name)
{
    printf("\n");
    printf("\t");
    printtab(name);
    printf("|\t");
}

void printline()
{
    printf("\n\t----------------|------------------------------------------------------------------------------------");
}

void PrintSubSelectionInfo(SubSelection* subsel)
{
    printf("Sub-Selection info:\n");
    printf("\tisUnqiue:%d\n",subsel->isUnique);
    printf("\tisAllColumn:%d\n",subsel->isAllColumn);

    printhead("Colunms List");
    printtab("column");
    printtab("table");
    printline();
    ColumnList* col = subsel->columns;
    while(col!=NULL)
    {
        printhead("");
        printtab(col->columnName);
        printtab(col->belongTable);
        col = col->next;
    }
    printf("\n");

    printhead("FromTable List");
    printtab("nick name");
    printtab("real name");
    printline();
    FromList* tb= subsel->froms;
    while(tb!=NULL)
    {
        printhead("");
        printtab(tb->nickname);
        printtab(tb->tbname);
        tb = tb->next;
    }
    printf("\n");


    printhead("Conditions");
    printtab("column");
    printtab("table");
    printtab("data");
    printtab("datatype");
    printtab("operator");
    printline();
    Condition* cond= subsel->conds;
    while(cond!=NULL)
    {
        printhead("");
        printtab(cond->columnName);
        printtab(cond->tbname);
        printtab(cond->data);
        char buf[50];
        sprintf(buf,"%d",cond->dataType);
        printtab(buf);
        sprintf(buf,"%d",cond->op);
        printtab(buf);
        cond = cond->next;
    }
    printf("\n");

    printhead("Join List");
    printtab("column1");
    printtab("table1");
    printtab("column2");
    printtab("table2");
    printtab("operator");
    printline();
    Joins* join= subsel->joins;
    while(join!=NULL)
    {
        printhead("");
        printtab(join->col1);
        printtab(join->tbname1);
        printtab(join->col2);
        printtab(join->tbname2);
        char buf[50];
        sprintf(buf,"%d",join->op);
        printtab(buf);
        join = join->next;
    }
    printf("\n");
}

char *strdupxy(char *s)
{
    char *p;
    p = (char *) malloc(strlen(s) + 1);
    if (p != NULL)
        strcpy(p, s);
    return p;
}
