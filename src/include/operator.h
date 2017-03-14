#include "recordmanager.h"
/**************************
*Interfaces for SPJ and
***************************/


extern void* Output_Join(char* recbin1,char* recbin2,Table* tb1,Table* tb2);
extern char* GetNext_Join(void* (*Join_Method)(void*),Joins* joins);
extern void StartJoin(void* (*Join_Method)(void*),Joins* joins);
extern void Output_Filter();
extern void GetNext_Filter();

extern void Output_Prj();
extern void GetNext_Prj();
