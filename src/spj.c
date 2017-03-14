#include "include/global.h"
#include "include/spj.h"
#include "include/recordmanager.h"
#include "include/operator.h"

int isMatchJoins(char* recbin1,char* recbin2,Joins* joins);
char* _MBuffer;
/***************************************
**Different Methods for JOIN operator***
****************************************/
/*********Nest Loop Join **************/
Table* tb_big;
Table* tb_small;
int Order;   // 0:tb1=big   1:tb1=small
int init_nestloop(Joins* joins)
{
    Order =0;
    Table* tb1 = OpenTable("newdb",joins->tbname1,NULL);
    Table* tb2 = OpenTable("newdb",joins->tbname2,NULL);

    if(tb1->stt->recordCnt == 0 || tb2->stt->recordCnt == 0)
        return -1;

    if(tb1->stt->maxblkID > tb2->stt->maxblkID)
    {
        tb_big = tb1;
        tb_small = tb2;
        Order = 0;
    }
    else
    {
        tb_big = tb2;
        tb_small = tb1;
        Order = 1;
    }
    _MBuffer= (char*)malloc(MAXBLOCKS_SPJ*BLOCKSIZE);
    return 0;
}

int fillBlocks(int startBlkID,int MaxBlkID)
{
    int cnt = 0;
    char* ibuf;
    while(cnt<=MAXBLOCKS_SPJ && (startBlkID+cnt)<=MaxBlkID)
    {
        ibuf = _MBuffer+(cnt*BLOCKSIZE);
        LoadPhyBlk(ibuf,startBlkID+cnt,tb_small->dataTable);
        cnt++;
    }
    return cnt;
}

void* Join_nestloop(Joins* joins)
{
    Pthread_detach(Pthread_self());
    init_nestloop(joins);
    int useblkcnt = 0;
    int tagSmallBlkID = 0;
    int outputCnt = 0;
    // loop to fill the M Block with the small table
    while(1)
    {
        useblkcnt = fillBlocks(tagSmallBlkID,tb_small->stt->maxblkID);
        if(useblkcnt==0)
        {
            Output_Join(NULL,NULL,NULL,NULL);
            free(_MBuffer);
            return NULL;
        }
        //for each fill, the record of big table will scan evrey block.
        int bigRecID;
        char* bigRecBuf = (char*)malloc(tb_big->recordbinary_length);
        for(bigRecID = 0; bigRecID <= tb_big->stt->maxRecordID; bigRecID++)
        {
            LookupRecord_MM(bigRecBuf,bigRecID,tb_big);
            //scan every block from M blocks
            int i;
            int j;
            for(i = 0; i<useblkcnt; i++)
            {
                char* myblk = _MBuffer + (i*BLOCKSIZE);
                BlkHeader* header = (BlkHeader*)myblk;
                int myreccnt = header->recordNum;

                OffsetTable* offtb = (OffsetTable*)(myblk + sizeof(BlkHeader));
                OffsetItem* offitem;
                char* curSmallRec;
                // scan every record in a block
                for(j=0; j<myreccnt; j++)
                {
                    offitem = offtb + j;
                    if(!offitem->isAvail)
                        continue;
                    curSmallRec = myblk + offitem->offset;

                    // condtions
                    if(Order)
                    {
                        if(isMatchJoins(curSmallRec,bigRecBuf,joins))
                        {
                            Output_Join(curSmallRec,bigRecBuf,tb_small,tb_big);// output
                            outputCnt++;
                        }
                    }
                    else
                    {
                        if(isMatchJoins(bigRecBuf,curSmallRec,joins))
                        {
                            Output_Join(bigRecBuf,curSmallRec,tb_big,tb_small);
                            outputCnt++;
                        }
                    }
                }
            }
        }
        tagSmallBlkID += useblkcnt;
    }
    Output_Join(NULL,NULL,NULL,NULL);
    free(_MBuffer);
    //return outputCnt;
    return NULL;
}

int isMatchJoins(char* recbin1,char* recbin2,Joins* joins)
{
    Joins* curJoin = joins;
    while(curJoin!=NULL)
    {
        if(curJoin->coltype1 == INT)
        {
            int val1 = *(int*)(recbin1+curJoin->off_col1);
            int val2 = *(int*)(recbin2+curJoin->off_col2);
            switch(curJoin->op)
            {
            case EQ:
                if(val1 == val2)    break;
                else return 0;
            case GE:
                if(val1 >= val2)    break;
                else return 0;
            case LE:
                if(val1 <= val2)    break;
                else return 0;
            case GT:
                if(val1 > val2)    break;
                else return 0;
            case LT:
                if(val1 < val2)    break;
                else return 0;
            case NE:
                if(val1 != val2)    break;
                else return 0;
            }
        }
        else if (curJoin->coltype1 == CHAR)
        {
            char* cval1 = recbin1+curJoin->off_col1;
            char* cval2 = recbin2+curJoin->off_col2;
            int i;
            for(i=0; i<curJoin->colsize1; i++)
            {
                if(cval1[i]!=cval2[i])
                    return 0;
            }
        }

        curJoin=curJoin->next;
    }
    return 1;
}
