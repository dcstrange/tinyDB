#ifndef _BUFFERMANAGER_H
#define _BUFFERMANAGER_H 1

#include "metaUtility.h"

typedef struct
{
    unsigned int db_addr;   // range: 0~4GB
    unsigned long cur_addr; // range: 0~16K*TB  note:this addr can be both in Disk and MainMemory.
    char pos_flag;  // this flag is illustrate what the record store position type is, MainMemory or Disk.
    struct AddrTransItem* next_item;
} AddrTransTable,AddrTransItem;

#endif
