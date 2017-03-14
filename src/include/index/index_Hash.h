#ifndef _HASH_H
#define _HASH_H 1

#define BUCKETNUM 1000000     //one million buckets will cost near 10M space.
enum hashkeytype {NUMERIC,STR};
int _HashKeyType;
typedef struct hashitem
{
    void* hashkey;
    int recordID;
    struct hashitem* next;
} Hashitem;

typedef struct
{
    Hashitem* firstitem;
    Hashitem* enditem;
} Hashbucket;

typedef struct
{
    int (*getfunc)(void*,Hashbucket*);
    int (*insertfunc)(void*,int,Hashbucket*);
    int (*removefunc)(void*,Hashbucket*);
}HashUtils;

extern Hashbucket* init_hash(int hashkeytype);

extern int gethashitem(void* hashkey,Hashbucket* bucketgroup);

extern int inserthashitem(void* hashkey,int recordID,Hashbucket* bucketgroup);

extern int removehashitem(void* hashkey, Hashbucket* bucketgroup);

#endif
