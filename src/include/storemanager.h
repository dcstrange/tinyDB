#ifndef _STOREMANAGER_H
#define _STOREMANAGER_H 1
#include <stdio.h>
#include "global.h"

/***** Interfaces for Buffer Manager *****/
extern int LoadPhyBlk(char* buf, int fileBlkID,FILE* fd);
extern int WritePhyBlk(char*buf,int fileBlkID,FILE* fd);
#endif
