#include "../include/index/index_Hash.h"
#include <libio.h>
HashUtils myUtils;
HashUtils* myHashUtils;

Hashbucket* myBuckets;

/* IF NUMERIC TYPE */
unsigned int hash_numeric(int key)
{
    return (unsigned)key%BUCKETNUM;
}

int getfunc_num(void* hashkey,Hashbucket* bucketgroup)
{
    int mykey = *(int*)hashkey;
    int bucketid = hash_numeric(mykey);
    Hashitem* item = bucketgroup[bucketid].firstitem;
    while(item != NULL)
    {
        if(*(int*)(item->hashkey) == mykey)
            return item->recordID;
        item = item->next;
    }
    return -1;
}

int insertfunc_num(void* hashkey,int recordID,Hashbucket* bucketgroup)
{
    Hashitem* newitem = (Hashitem*)malloc(sizeof(Hashitem));
    newitem->hashkey = hashkey;
    newitem->recordID = recordID;
    newitem->next=NULL;

    int mykey = *(int*)hashkey;
    int bucketid = hash_numeric(mykey);
    Hashbucket* mybucket = bucketgroup+bucketid;
    if(mybucket->firstitem==NULL)
    {
        mybucket->firstitem = newitem;
        mybucket->enditem = newitem;
    }
    else
    {
        mybucket->enditem->next = newitem;
        mybucket->enditem = newitem;
    }
    return 0;
}

int removefunc_num(void* hashkey, Hashbucket* bucketgroup)
{
    int mykey = *(int*)hashkey;
    int bucketid = hash_numeric(mykey);
    Hashbucket* mybucket = bucketgroup+bucketid;
    Hashitem* item = mybucket->firstitem;
    if(item == NULL)
        return -1;
    else if(*(int*)item->hashkey == mykey)
    {
        mybucket->firstitem = item->next;
        if(mybucket->enditem == item)
            mybucket->enditem = NULL;
        free(item);
        return 0;
    }
    else
    {
        while(item->next!=NULL && *(int*)(item->next->hashkey) != mykey )
        {
            item = item->next;
        }
        if(item->next == NULL)
            return -1;
        else
        {
            Hashitem* tmpitem = item;
            item=item->next;
            tmpitem->next = item->next;
            if(item->next==NULL)
                mybucket->enditem=tmpitem;
            free(item);
            return 0;
        }
    }

    return NULL;
}

/*ENDIF*/

/*IF STRING TYPE */
int hash_string(char* key);
Hashitem* getfunc_str(void* hashkey,Hashbucket* bucketgroup) {}

Hashitem* insertfunc_str(void* hashkey,Hashbucket* bucketgroup) {}

Hashitem* removefunc_str(void* hashkey, Hashbucket* bucketgroup) {}
/*ENDIF*/

/*******************************
**Interfaces for Record Manager*
********************************/

Hashbucket* init_hash(int hashkeytype)
{
    int type = hashkeytype;
    switch(type)
    {
    case NUMERIC:
        myUtils.getfunc= getfunc_num;
        myUtils.insertfunc=insertfunc_num;
        myUtils.removefunc = removefunc_num;
        break;
    case STR:
        myUtils.getfunc= getfunc_str;
        myUtils.insertfunc=insertfunc_str;
        myUtils.removefunc = removefunc_str;
        break;
    }
    myHashUtils = &myUtils;

    Hashbucket* buckets = (Hashbucket*)calloc(BUCKETNUM,sizeof(Hashbucket));
    return buckets;
}

int gethashitem(void* hashkey,Hashbucket* bucketgroup)
{
    return myUtils.getfunc(hashkey,bucketgroup);
}

int inserthashitem(void* hashkey,int recordID,Hashbucket* bucketgroup)
{
    return myUtils.insertfunc(hashkey,recordID,bucketgroup);
}

int removehashitem(void* hashkey, Hashbucket* bucketgroup)
{
    return myUtils.removefunc(hashkey,bucketgroup);
}
