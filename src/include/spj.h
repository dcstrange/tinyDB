#ifndef _SPJ_H
#define _SPJ_H
#define MAXBLOCKS_SPJ 10
#include "recordmanager.h"

/***************************************
**Different Methods for JOIN operator***
****************************************/
extern void* Join_nestloop(Joins* joins);

#endif
