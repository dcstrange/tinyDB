#include "include/operator.h"
#include "include/csapp.h"

/*****************************
**interfaces for JOIN operator
*****************************/
static char* JoinBuf;
StreamDataItem* JoinHead;
StreamDataItem* JoinTail;
static sem_t _Mutex;   //当对JoinHead指针有方位且更改动作时,需要加锁.
static isStart = 0;


void* _initOperatorEnv()
{
    Sem_init(&_Mutex,0,1);
    JoinHead = JoinTail = NULL;
    return NULL;
}

void _startJoin(void* (*Join_Method)(void*),Joins* joins)
{
    _initOperatorEnv();
    pthread_t tid;
    Pthread_create(&tid,NULL,Join_Method,(void*)joins);
}

void* Output_Join(char* recbin1,char* recbin2,Table* tb1,Table* tb2)
{
    StreamDataItem* streamData = (StreamDataItem*)malloc(sizeof(StreamDataItem));
    /************ 不完善, 没有考虑到JoinBuf的大小 *************/
    if(recbin1 == NULL || recbin2 == NULL || tb1==NULL || tb2 == NULL)
    {
        /* Flag of Finish to Join */
        streamData->data = NULL;
        streamData->next = NULL;
    }
    else
    {
        char* data = (char*)malloc(tb1->recordbinary_length+tb2->recordbinary_length);
        memcpy(data,recbin1,tb1->recordbinary_length);
        memcpy(data+tb1->recordbinary_length,recbin2,tb2->recordbinary_length);
        streamData->data = data;
        streamData->next = NULL;
    }
    P(&_Mutex);
    if(JoinHead==NULL)
    {
        JoinHead = JoinTail = streamData;
    }
    else
    {
        JoinTail->next = streamData;
    }
    V(&_Mutex);
    return NULL;
}

char* GetNext_Join(void* (*Join_Method)(void*),Joins* joins)
{
    if(!isStart)
    {
        _startJoin(Join_Method,joins);
        isStart=1;
    }
    while(JoinHead == NULL)
    {
        /*spin*/
    }

    if(JoinHead->data==NULL)
    {
        //finish
        isStart=0;
        return NULL;
    }
    P(&_Mutex);
    StreamDataItem* streamdata = JoinHead;
    JoinHead = JoinHead->next;
    V(&_Mutex);
    return streamdata->data;

}




/*******************************
**interfaces for FILTER operator
********************************/
int isMatchConds(char* recbin,Condition* conds,Table* tb1,Table* tb2)
{
    int loc;
    Condition* cond = conds;
    while(cond!=NULL)
    {
        if(cond->tbID==1)
        {
            loc = cond->colOff;
        }
        else
        {
            loc = cond->colOff + tb1->recordbinary_length;
        }
        if(cond->dataType == INT)
        {
            int cmpval = atoi(cond->data);
            int recval = *(int*)(recbin+loc);
            switch(cond->op)
            {
            case EQ:
                if(recval == cmpval)    break;
                else return 0;
            case GE:
                if(recval >= cmpval)    break;
                else return 0;
            case LE:
                if(recval <= cmpval)    break;
                else return 0;
            case GT:
                if(recval > cmpval)    break;
                else return 0;
            case LT:
                if(recval < cmpval)    break;
                else return 0;
            case NE:
                if(recval != cmpval)    break;
                else return 0;
            }
        }
        else if(cond->dataType == CHAR)
        {
            return 0;  /*******不完善******/
        }

        cond = cond->next;
    }

    return 1;
}
