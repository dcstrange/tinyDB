#include "include/storemanager.h"
#include <stdio.h>



/********/
int LoadPhyBlk(char* buf, int fileBlkID,FILE* fd)
{
    size_t rsize;
    int cnt = 0;
    do
    {
        fseek(fd,fileBlkID*BLOCKSIZE,SEEK_SET);
        rsize=fread(buf,1,BLOCKSIZE,fd);
        cnt++;
    }
    while(rsize < BLOCKSIZE && cnt<10);
    if(rsize < BLOCKSIZE)
        return -1;
    return 0;
}

int WritePhyBlk(char*buf,int fileBlkID,FILE* fd)
{
    size_t wsize;
    int cnt = 0;
    do
    {
        fseek(fd,fileBlkID*BLOCKSIZE,SEEK_SET);
        wsize=fwrite(buf,1,BLOCKSIZE,fd);
        cnt++;
    }
    while(wsize < BLOCKSIZE && cnt<10);
    if(wsize < BLOCKSIZE)
        return -1;
    return 0;
}
