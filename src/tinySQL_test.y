%{
extern int yylex();
extern int yyerror();
extern int lex_init();
#include "include/global.h"	
#include "include/metaUtility.h"
#include "include/recordmanager.h"
#include <stdio.h>

char* tbname;
static unsigned int attrcnt;
static attrinfo attrs[_MAXNUM_TB_ATTR];

static int funcRnt;

static int curPos;
static char recordstr[4096];

Condition cond[20];
int condcnt;

static char errmsg[4096];

//static Table* _proctable;

//static FILE* LOG;


/***********
* Functions
************/
static int SelCnt;
//static SubSelection* curSelection;
//static int curColCnt;
//static int curCondCnt;
//static int curTmpTBCnt;

static SubSelection* SelStackTop;
SubSelection* yyPush_Selecttion();
SubSelection* yyPop_Selecttion();
void yyAddSelCol(char* belongTB, char* colName);
void yyAddSelTB(char* tbname,char* nickname);
void yyAddSelJoin(char* tbname1,char* col1,char* tbname2,char* col2,int op);
void yyAddSelConditions(char* tbname,char* columnName,char* data,int dataType,int op);
int optrans(char* opstr);
%}

%union{
	int intval;
	char* strval;
}

%start sql_start

%token <strval> NAME
%token <strval> STRING
%token <strval> NUMBER
%token <strval> COMPARISION

	/* operator */
%left	 AND
%left COMPARISION /* < <= > >= <> = */
%left '+' '-'
%left '*' '/'

	/* reserved keywords */
%token SELECT FROM WHERE ORDER BY ASC DESC
%token ALL UNIQUE DISTINCT
%token CREATE TABLE DROP INDEX
%token INSERT INTO VALUES DELETE
%token CHARACTER INTEGER DATE
%token SHOW TABLES
%token EXIT

%%

sql_start:
		sql_func
		{
			if(funcRnt == 0)
				printf("> Success.\n");
			else if(funcRnt < 0)
			{
				printf("> Failure:%d> %s\n",funcRnt,errmsg);
			}
			return 0;
		}
	|	EXIT
		{
			if(CloseAllTable()!=0)
			{
				printf("> Failure: cannot flush all tables..\n");
				return 0;
			}
			return -1;
		}
	;
sql_func:
		';'
		{
			funcRnt = 100;
		}
	|	table_def
	|	table_drop
	|	insert_stat
	|	delete_stat
	|	select_stat
	;
/* create table */
table_def:
		CREATE TABLE table '(' table_attr_list ')' ';'
		{
			funcRnt = CreateTable(tbname, attrcnt, attrs,errmsg);		
		}
	;

table:
		NAME
		{
			tbname = $1;
		}
	;
table_attr_list:
		column_def
	|	table_attr_list ',' column_def
	;
column_def:
		column data_type
		{
			attrcnt++;
		}
	;
column:
		NAME
		{
			attrs[attrcnt].name = $1;
			cond[condcnt].columnName = $1;
		}
	;
data_type:
		CHARACTER '(' NUMBER ')'
		{
			attrs[attrcnt].type = CHAR;
			attrs[attrcnt].size_used = atoi($3);
		}
	|	INTEGER
		{
			attrs[attrcnt].type = INT;
			attrs[attrcnt].size_used = 1;
		}
	|	DATE
		{
			attrs[attrcnt].type = DATE;
			attrs[attrcnt].size_used = 1;
		}
	;
/* drop table */
table_drop:
		DROP TABLE table ';'
		{
			funcRnt = DropTable(tbname,errmsg);
		}
	;
/**************************************
**About Records operations Statements
**************************************/
/* insert statements */
insert_stat:
		INSERT INTO table VALUES '(' insert_list ')' ';'
		{
			funcRnt = InsertRecord("newdb",tbname,recordstr,errmsg);
		}
	; 	
insert_list:
		NUMBER
		{
			strcpy(recordstr+curPos, $1);
			curPos+=strlen($1);
		}
	|	STRING
		{
			strcpy(recordstr+curPos, $1);
			curPos+=strlen($1);
		}
	|	insert_list ',' NUMBER
		{
			strcpy(recordstr+curPos, ",");
			curPos++;

			strcpy(recordstr+curPos, $3);
			curPos += strlen($3);
		}
	|	insert_list ',' STRING
		{
			strcpy(recordstr+curPos, ",");
			curPos++;

			strcpy(recordstr+curPos, $3);
			curPos += strlen($3);
		}
	;
	/* delete statement */
delete_stat:
		DELETE FROM table where_clause ';'
		{
			funcRnt = DeleteRecord(tbname,SelStackTop->conds,errmsg);
		}
	;


/* select statements */
select_stat:
		select_seg ';'
		{
			//printf("Finish\n");
		}
	;
select_seg:
		select_clause FROM fromlist where_clause
		{
			char* tmptbname = (char*)malloc(7);		//set a tmp tbname for the catch table of selection results.
			sprintf(tmptbname,"%s%d","TMPTB_",SelCnt);
			
			SubSelection* subsel = yyPop_Selecttion();	//pop when a single selecion parsing finished.  
			//printf("pop..\n");			

			SelectRecord(subsel,tmptbname,errmsg);			////////////////////////////////////////
			//printf("-----one sub selection end----\n");
		}
	;
select_clause:
		selectbegin unique sellist
		{
			SelStackTop->isAllColumn = 0;
		}
	|	selectbegin unique '*'
		{
			SelStackTop->isAllColumn = 1;
		}
	;
selectbegin:
		SELECT
		{
			//printf("-----one sub selection begin----\n");
			yyPush_Selecttion();	//begin a new sub-selection
		}
	;
unique:
		/* empty */
		{
			SelStackTop->isUnique = 0;
		}
	|	ALL
		{
			SelStackTop->isUnique = 0;
		}
	|	DISTINCT
		{
			SelStackTop->isUnique = 1;
		}
	|	UNIQUE
		{
			SelStackTop->isUnique = 1;
		}
	;
sellist:
		sel_column
	|	sellist ',' sel_column
	;
sel_column:
		NAME
		{
			yyAddSelCol(NULL,$1);
		}
	|	NAME '.' NAME
		{
			yyAddSelCol($1,$3);
		}
	;		
fromlist:
		sel_table
	|	fromlist ',' sel_table
	;
sel_table:
		NAME
		{	
			yyAddSelTB($1,$1);
			
		}
		/* sub selection */
	|	'(' select_seg ')' /* +empty */			
		{	
			char* tmptbname = (char*)malloc(8);		//set a tmp tbname for the catch table of selection results.
			sprintf(tmptbname,"%s%d","TMPTB_",SelCnt++);
			yyAddSelTB(tmptbname,tmptbname);
		}
	|	'(' select_seg ')' NAME			
		{	
			char* tmptbname = (char*)malloc(7);		//set a tmp tbname for the catch table of selection results.
			sprintf(tmptbname,"%s%d","TMPTB_",SelCnt++);
			
			yyAddSelTB(tmptbname,$4);
		}
		;	
where_clause:
		/* empty */
	|	WHERE condition
	;
condition:
		expr
	|	condition AND expr
	;
expr:
		NAME '.' NAME COMPARISION NAME '.' NAME
		{
			yyAddSelJoin($1,$3,$5,$7,optrans($4));
		}
	|	NAME COMPARISION NUMBER
		{
			yyAddSelConditions(NULL,$1,$3,INT,optrans($2));
		}
	|	NAME COMPARISION STRING
		{
			yyAddSelConditions(NULL,$1,$3,CHAR,optrans($2));
		}
	|	NAME '.' NAME COMPARISION NUMBER
		{
			yyAddSelConditions($1,$3,$5,INT,optrans($4));
		}
	|	NAME '.' NAME COMPARISION STRING
		{
			yyAddSelConditions($1,$3,$5,CHAR,optrans($4));
		}
	;
%%


void initSelection()
{
	//curColCnt = 0;
	//curCondCnt = 0;
	SelStackTop = NULL;
}


void parser_init()
{
	lex_init();
	attrcnt = 0;
	tbname = NULL;
	funcRnt = 0;
	curPos = 0;
	condcnt = 0;

	SelCnt = 0;
	//curTmpTBCnt = 0;
	initSelection();

	while(SelStackTop!=NULL){ yyPop_Selecttion();}
	return;
}



SubSelection* yyPush_Selecttion()
{
	SubSelection* subSel = (SubSelection*)calloc(1,sizeof(SubSelection));
	subSel->parent = SelStackTop;
	SelStackTop = subSel;
	return SelStackTop;
}

SubSelection* yyPop_Selecttion()
{
	if(SelStackTop==NULL)
		return NULL;
	SubSelection* popSel = SelStackTop;
	SelStackTop = popSel->parent;
	return popSel;
}

void yyAddSelCol(char* belongTB, char*colName)
{
	ColumnList* newCol = (ColumnList*)malloc(sizeof(ColumnList));
	newCol->belongTable = belongTB;
	newCol->columnName = colName;
	newCol->next = NULL;

	ColumnList* tmpCol = SelStackTop->columns;
	if(tmpCol == NULL)
		SelStackTop->columns = newCol;
	else
	{
		while(tmpCol->next != NULL)
		{
			tmpCol = tmpCol->next;
		}
		tmpCol->next = newCol;
	}
}                          

void yyAddSelTB(char* tbname,char* nickname)
{
	FromList* newtb = (FromList*)malloc(sizeof(FromList));
	newtb->tbname = tbname;
	newtb->nickname = nickname;
	newtb->next = NULL;
	
	FromList* tmpTb = SelStackTop->froms;
	if(tmpTb == NULL)
		SelStackTop->froms = newtb;
	else
	{
		while(tmpTb->next != NULL)
		{
			tmpTb = tmpTb->next;
		}
		tmpTb->next = newtb;
	}
}

void yyAddSelJoin(char* tbname1,char* col1,char* tbname2,char* col2,int op)
{
	Joins* newjoin = (Joins*)malloc(sizeof(Joins));
	newjoin->tbname1 = tbname1;
	newjoin->col1 = col1;
	newjoin->tbname2 = tbname2;
	newjoin->col2 = col2;
	newjoin->op = op;
	
	Joins* tmpjoin = SelStackTop->joins;
	if(tmpjoin == NULL)
		SelStackTop->joins = newjoin;
	else
	{
		while(tmpjoin->next != NULL)
		{
			tmpjoin = tmpjoin->next;
		}
		tmpjoin->next = newjoin;
	}
}

void yyAddSelConditions(char* tbname,char* columnName,char* data,int dataType,int op)
{
	if(SelStackTop==NULL)
	{
		SelStackTop = (SubSelection*)calloc(1,sizeof(SubSelection));
	}
	Condition* newcond = (Condition*)malloc(sizeof(Condition));
	newcond->tbname = tbname;
	newcond->columnName = columnName;
	newcond->data = data;
	newcond->dataType = dataType;
	newcond->op = op;
	
	Condition* tmpcond = SelStackTop->conds;
	if(tmpcond == NULL)
		SelStackTop->conds = newcond;
	else
	{
		while(tmpcond->next != NULL)
		{
			tmpcond = tmpcond->next;
		}
		tmpcond->next = newcond;
	}
}

int optrans(char* opstr)
{
	if (strcmp(opstr, "=") == 0) return EQ;
	else if (strcmp(opstr, ">=") == 0) return GE;
	else if (strcmp(opstr, "<=") == 0) return LE;
	else if (strcmp(opstr, ">") == 0) return GT;
	else if (strcmp(opstr, "<") == 0) return LT;
	else if (strcmp(opstr, "<>") == 0) return NE;
	return -1;
}
