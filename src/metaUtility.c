#include <fcntl.h>    // open/access mode
#include <sys/stat.h> // mkdir
#include <unistd.h>   //access file/dir
#include <stdlib.h>
#include "include/metaUtility.h"
/*** Relevant Utilities ***/
/* The meta file of a table is in the following form.
>>>>
    flag |
    table_name |
    [(attr_name,attr_type),...] |
    primarykey_idx[] |
    [(index_attr,index_type),...]
<<<<
   For the fast to get the position of the item in this form, we define a enum following.
*/
int tb_format2des(char *linebuffer,Table* tb);
char** split(char* str, const char* spl, unsigned int max_part_num, unsigned int* n);
void freeStringList(char** list,int stringNum);
void tb_des_parse(char* desline,Table *tb);
int record_binary_lenth_predict(Table* tbmeta);
int tb_record_binary_length_set(Table* tb);
int attrvalue_serialize_by_type(char* buf,char* str,int type,unsigned int num);
int record_string_serialize(char* buf,char* recordstr,Table* tbmeta,char* errmsg);

/***** Schema create *****/
int db_create(db_attr* dbinfo);
int tb_create(Table* table);

/***** The meta data IO *****/
int tb_schema_flush(Table* table,FILE* fmeta);
int tb_statistic_flush(MetaInfo* meta,FILE* fmeta);
int tb_meta_init(Table* tb,FILE* fmeta);

int tb_schema_load(Table* tb,FILE* fmeta);
int tb_statistic_load(MetaInfo* meta,FILE* fmeta);

/***** Get the table revalent files *****/
FILE* tb_des_open(char* dbname,char* tbname,const char* modes);
FILE* tb_datatable_open(char* dbname,char* tbname,const char* modes);


void db_dir_path(char* buf,char* dbname)
{
    sprintf(buf,"%s/%s",_DBMSDIR,dbname);
}

void tb_metafile_path(char* buf,char* dbname,char* tbname)
{
    sprintf(buf,"%s/%s/%s/%s_meta",_DBMSDIR,dbname,_TABLEMETADIR,tbname);
}

void tb_datafile_path(char* buf,char* dbname,char* tbname)
{
    sprintf(buf,"%s/%s/%s",_DBMSDIR,dbname,tbname);
}

void db_attr_init(db_attr* dbinfo,char *name)
{
    if(dbinfo== NULL)
        return;
    dbinfo->dbname = name;
}

/*Create a Database with detail*/
int db_create(db_attr* dbinfo)
{
    if(dbinfo==NULL)
        return -1;

    char dbpath[_MAXLEN_DBPATH];
    sprintf(dbpath,"%s/%s",_DBMSDIR,dbinfo->dbname);

    int r = mkdir(dbpath,S_IRWXU);
    return r;
}

int tb_create(Table* tb)
{
    /* get the db path by name */
    char dbpath[_MAXLEN_DBPATH];
    db_dir_path(dbpath,tb->belongDB);

    char tablepath[_MAXLEN_TABLEPATH];
    tb_datafile_path(tablepath,tb->belongDB,tb->tb_name);

    /* check if path validate and if no same tablename, then create one*/
    if(access(dbpath,F_OK)<0) return -2;
    if(access(tablepath,F_OK)==0) return -3;
    if(creat(tablepath,S_IRWXU)<0) return -1;

    FILE* fmeta = tb_des_open(tb->belongDB,tb->tb_name,"wb+");
    tb_meta_init(tb,fmeta);
    fclose(fmeta);
    return 0;
}

/* open the tabel descript file of a database */
FILE* tb_des_open(char* dbname,char* tbname,const char* modes)
{
    char dbpath[_MAXLEN_DBPATH];
    db_dir_path(dbpath,dbname);
    if(access(dbpath,F_OK)<0) return NULL;

    char metapath[_MAXLEN_TABLEPATH+1];
    sprintf(metapath,"%s/%s/%s",_DBMSDIR,dbname,_TABLEMETADIR);
    if(access(metapath,F_OK)<0)
        if(mkdir(metapath,S_IRWXU)<0) return NULL;

    char path[_MAXLEN_TABLEPATH + 1];
    tb_metafile_path(path,dbname,tbname);
    FILE *fmeta = fopen(path,modes);
    return fmeta;
}

FILE* tb_datatable_open(char* dbname,char* tbname,const char* modes)
{
    char dbpath[_MAXLEN_DBPATH];
    db_dir_path(dbpath,dbname);
    if(access(dbpath,F_OK)<0) return NULL;

    char path[_MAXLEN_TABLEPATH + 1];
    tb_datafile_path(path,dbname,tbname);
    FILE *fmeta = fopen(path,modes);
    return fmeta;
}

int tb_schema_flush(Table* table,FILE* fmeta)
{
    char* despbuf = (char*)calloc(1,BLOCKSIZE);
    tb_des_serialize(despbuf,table);

    fseek(fmeta,0,SEEK_SET);
    int rnt = fwrite(despbuf,BLOCKSIZE,1,fmeta);
    free(despbuf);
    return rnt;
}

int tb_statistic_flush(MetaInfo* meta,FILE* fmeta)
{
    fseek(fmeta,BLOCKSIZE,SEEK_SET);
    TableRecordStt tbStt;
    tbStt.recordCnt = meta->recordCnt;
    tbStt.maxRecordID = meta->maxRecordID;
    tbStt.maxblkID = meta->maxblkID;
    char* buf = (char*)calloc(1,BLOCKSIZE);
    memcpy(buf,&tbStt,sizeof(TableRecordStt));
    int rnt = fwrite(buf,BLOCKSIZE,1,fmeta);
    free(buf);
    if(rnt<1)
    {
        printf("Error: flush statistic failure");
        return -1;
    }

    fseek(fmeta,2*BLOCKSIZE,SEEK_SET);
    int wn = fwrite(meta->myMap,sizeof(MapItem),meta->maxRecordID+1,fmeta);
    if(wn<meta->maxRecordID+1)
    {
        printf("Error: flush statistic failure");
        return -1;
    }
    return 0;
}

int tb_schema_load(Table* tb,FILE* fmeta)
{
    char* buf = (char*)malloc(BLOCKSIZE);
    fseek(fmeta,0,SEEK_SET);
    if(fread(buf,BLOCKSIZE,1,fmeta)<1)
        return -1;
    tb_des_deserialize(buf,tb);
    return 0;
}

int tb_statistic_load(MetaInfo* meta,FILE* fmeta)
{
    char* buf = (char*)malloc(BLOCKSIZE);
    fseek(fmeta,BLOCKSIZE,SEEK_SET);
    if(fread(buf,BLOCKSIZE,1,fmeta)<1)
        return -1;

    TableRecordStt* tbStt = (TableRecordStt*)buf;
    meta->maxblkID = tbStt->maxblkID;
    meta->maxRecordID = tbStt->maxRecordID;
    meta->recordCnt = tbStt->recordCnt;

    /* load the Map */
    fseek(fmeta,2*BLOCKSIZE,SEEK_SET);
    int rn = fread(meta->myMap,sizeof(MapItem),meta->maxRecordID+1,fmeta);
    if(rn<meta->maxRecordID+1)
    {
        printf("Error: load statistic failure");
        return -1;
    }
    return 0;
}

int tb_meta_init(Table* tb,FILE* fmeta)
{
    MetaInfo tbStt;
    tbStt.recordCnt = 0;
    tbStt.maxRecordID = -1;
    tbStt.maxblkID = -1;

    tb_schema_flush(tb,fmeta);
    tb_statistic_flush(&tbStt,fmeta);
    return 0;
}

int changeFileSize(char* filePath,size_t fsize)
{
    int tc = truncate(filePath,fsize);
    return tc;
}

int tb_des_serialize(char *linebuffer,Table* tb)
{
    char *flag = "0";
    char *tbname = tb->tb_name;

    unsigned int attrnum = tb->attr_num;
    unsigned int i;
    char* muti_attrinfo = (char*)calloc(1,_MAXLEN_META_TXTLINE);
    attrinfo* attrs = tb->attrs;
    for(i=0; i<attrnum; i++)
    {
        char one_attrinfo[_MAXLEN_TABLENAME+10];
        sprintf(one_attrinfo,"%s %d@%d",attrs[i].name,attrs[i].type,attrs[i].size_used);
        if(i==0)
            sprintf(muti_attrinfo,"%s",one_attrinfo);
        else
            sprintf(muti_attrinfo,"%s,%s",muti_attrinfo,one_attrinfo);
    }

    unsigned int pkeynum = tb->primary_num;
    char* strpkeys = (char*)calloc(1,_MAXLEN_META_TXTLINE);
    for(i=0; i<pkeynum; i++)
    {
        if(i==0)
            sprintf(strpkeys,"%d",tb->prmkeys[i]);
        else
            sprintf(strpkeys,"%s,%d",strpkeys,tb->prmkeys[i]);
    }

    unsigned int idxnum = tb->index_num;
    char* muti_idxinfo=(char*)calloc(1,_MAXLEN_META_TXTLINE);
    for(i=0; i<idxnum; i++)
    {
        char one_idxinfo[_MAXLEN_TABLENAME+10];
        sprintf(one_idxinfo,"%d %d",(unsigned int*)(tb->index_attr[i]),tb->index_type[i]);
        if(i==0)
            sprintf(muti_idxinfo,"%s",one_idxinfo);
        else
            sprintf(muti_idxinfo,"%s,%s",muti_idxinfo,one_idxinfo);
    }

    int charcnt = sprintf(linebuffer,"%s|%s|%s|%s|%s\n",flag,tbname,muti_attrinfo,strpkeys,muti_idxinfo);
    free(muti_attrinfo);
    free(strpkeys);
    free(muti_idxinfo);
    return charcnt;
}

/* to parse the description into a Table structure*/
void tb_des_deserialize(char* desline,Table *tb)
{
    char** dist;
    int n;
    dist = split(desline,"|",10,&n);

    char* tbnamese_des = dist[DES_TBNAME];
    char* attrinfo_des = dist[DES_TBATTRINFO];
    char* pkeys_des = dist[DES_PKEYS];
    char* idxinfo_des = dist[DES_IDXINFO];

    tb->tb_name = tbnamese_des;

    /* pasre the attribute info*/
    int attr_num;
    char** attrinfo_splits = split(attrinfo_des,",",_MAXNUM_TB_ATTR,&attr_num);

    tb->attr_num = attr_num;

    attrinfo* attrs = (attrinfo*)malloc(attr_num*sizeof(attrinfo));
//    char** attr_names = (char**)malloc(attr_num*sizeof(char*));
//    unsigned int* attr_types = (unsigned int*)malloc(attr_num*sizeof(unsigned int));
//    unsigned int* attrsize_used= (unsigned int*)malloc(attr_num*sizeof(unsigned int));
    int i;
    for(i=0; i<attr_num; i++)
    {
        char** name_type = split(attrinfo_splits[i]," ",2,NULL);
        char* onename = name_type[0];
        attrs[i].name = onename;

        char** type_typeused = split(name_type[1],"@",2,NULL);
        attrs[i].type=atoi(type_typeused[0]);
        attrs[i].size_used=atoi(type_typeused[1]);

        free(name_type[1]);
        free(type_typeused[0]);
        free(type_typeused[1]);
        free(attrinfo_splits[i]);
    }

    tb->attrs = attrs;
    /* parse the primary keys*/
    int pkey_num;
    char** pkeys_splits = split(pkeys_des,",",_MAXNUM_TB_ATTR,&pkey_num);
    unsigned int* pkeys = (unsigned int*)malloc(pkey_num*sizeof(unsigned int));
    for(i = 0; i<pkey_num; i++)
    {
        pkeys[i]=atoi(pkeys_splits[i]);
        free(pkeys_splits[i]);
    }
    tb->primary_num = pkey_num;
    tb->prmkeys = pkeys;

    /* parse the index information*/
////////    int idx_num;
////////    char** idxinfo_splits = split(idxinfo_des,",",_MAXNUM_TB_ATTR,&idx_num);
////////    unsigned int* idx_attr = (unsigned int*)malloc(idx_num*sizeof(unsigned int));
////////    unsigned int* idx_type = (unsigned int*)malloc(idx_num*sizeof(unsigned int));
////////    for(i=0; i<idx_num; i++)
////////    {
////////        char** id_type = split(idxinfo_splits[i]," ",2,NULL);
////////
////////        idx_attr[i]=atoi(id_type[0]);
////////        idx_type[i]=atoi(id_type[1]);
////////
////////        free(id_type[0]);
////////        free(id_type[1]);
////////        free(id_type);
////////        free(idxinfo_splits[i]);
////////    }
////////    tb->index_attr = idx_attr;
////////    tb->index_type = idx_type;

    tb_record_binary_length_set(tb);
}

int attrvalue_serialize_by_type(char* buf,char* str,int type,unsigned int num)
{

    //INT,SHORT,LONG,FLOAT,DOUBLE,CHAR,BOOL,DATE,DATETIME
    size_t size;
    char* memaddr;
    switch(type)
    {
    case INT:
    {
        int value = atoi(str);
        size = sizeof(int);
        memaddr = &value;
        break;
    }
    case SHORT:
    {
        short value = (short)atoi(str);     // ??is here someting wrong??
        size = sizeof(short);
        memaddr = &value;
        break;
    }
    case LONG:
    {
        long value = atol(str);
        size = sizeof(long);
        memaddr = &value;
        break;
    }
    case FLOAT:
    {
        float value = atof(str);
        size = sizeof(float);
        memaddr = &value;
        break;
    }
    case DOUBLE:
    {
        double value = atof(str);
        size = sizeof(double);
        memaddr = &value;
        break;
    }
    case CHAR:
    {
        size = num*sizeof(char);
        size_t strLen = strlen(str);

        if(str[0]!='\'' || str[strLen-1]!= '\'')
            return -2;
        strLen -=2;
        int i;
        for(i=0; i<size; i++)
            buf[i] = ' ';
        memcpy(buf,str+1,size>strLen?strLen:size);
        return size;
    }
    case BOOL:
    {
        int value = atoi(str);
        size = sizeof(int);
        memaddr = &value;
        break;
    }
        //case DATE:
        //case DATETIME:
    }
    memcpy(buf,memaddr,size);
    return size;
}

int attrvalue_deserialize_by_type(char* buf,char* binary,int type,unsigned int num)
{
    size_t size;
    switch(type)
    {
    case INT:
    {
        size = sprintf(buf,"%d",*(int*)binary);
        break;
    }
    case SHORT:
    {
        size = sprintf(buf,"%d",*(short*)binary);
        break;
    }
    case LONG:
    {
        size = sprintf(buf,"%ld",*(long*)binary);
        break;
    }
    case FLOAT:
    {
        size = sprintf(buf,"%f",*(float*)binary);
        break;
    }
    case DOUBLE:
    {
        size = sprintf(buf,"%lf",*(double*)binary);
        break;
    }
    case CHAR:
    {
        size = num;
        memcpy(buf,binary,size);
        break;
    }
    case BOOL:
    {
        size = sprintf(buf,"%d",*(int*)binary);
        break;
    }
        //case DATE:
        //case DATETIME:
    }

    return size;
}

/* this is to serialize a string of record(in which each value splited by ',') by the table meta info. */
int record_string_serialize(char* buf,char* recordstr,Table* tb,char* errmsg)
{
    int attr_num = tb->attr_num;
    attrinfo* attrs = tb->attrs;
    int attrcnt;
    char** valus_splits = split(recordstr,",",attr_num,&attrcnt);
    if(attrcnt!=tb->attr_num)
    {
        sprintf(errmsg,"Checkout the number of attributes\n");
        return -1;

    }
    //char* binarybuf = (char*)malloc(tb->recordbinary_length);
    int off = 0,i,n;
    for(i=0; i<attr_num; i++)
    {
        char* attrval = *(valus_splits+i);
        n = attrvalue_serialize_by_type(buf+off,attrval,attrs[i].type,attrs[i].size_used);
        if(n<0)
        {
            sprintf(errmsg,"Check out the value <%s>\n",attrval);
            return n;
        }
        off+=n;
    }
    freeStringList(valus_splits,attr_num);
    return off;
}

int record_binary_deserialize(char* buf,char* recordbinary,Table* tb)
{
    /*** 用逗号分隔 ***/
    int attr_num = tb->attr_num;
    attrinfo* attrs = tb->attrs;
    int off=0,ptr=0;
    int i,n;
    for(i=0; i<attr_num; i++)
    {
        n = attrvalue_deserialize_by_type(buf+off,recordbinary+ptr,attrs[i].type,attrs[i].size_used);
        off+=n;
        *(buf+off) = (i==attr_num-1)?0:',';
        off++;
        ptr+=attrvalue_binary_size(attrs[i].type,attrs[i].size_used);
    }
    return off; //包括字符串结尾'\0'
}

int attrvalue_binary_size(int type,int nsize)
{
    int size;
    switch(type)
    {
    case INT:
        size=sizeof(int);
        break;
    case SHORT:
        size=sizeof(short);
        break;
    case LONG:
        size=sizeof(long);
        break;
    case FLOAT:
        size=sizeof(float);
        break;
    case DOUBLE:
        size=sizeof(double);
        break;
    case CHAR:
        size=nsize*sizeof(char);
        break;
    case BOOL:
        size=sizeof(int);
        break;
        //case DATE,case DATETIME
    }
    return size;
}

int record_binary_lenth_predict(Table* tb)
{
    int attr_num = tb->attr_num;
    attrinfo* attrs = tb->attrs;
    unsigned int total = 0;
    int i;
    for(i=0; i<attr_num; i++)
    {
        total+=attrvalue_binary_size(attrs[i].type,attrs[i].size_used);
    }

    return total;
}

int record_binary_attr_locate(char* attrname,Table* tb,int* attrsize,int* attrType)
{
    int attr_num = tb->attr_num;
    attrinfo* attrs = tb->attrs;
    unsigned int loc = 0;
    int i;
    for(i=0; i<attr_num; i++)
    {
        if(strcmp(attrname,attrs[i].name)==0)
            break;
        loc+=attrvalue_binary_size(attrs[i].type,attrs[i].size_used);
    }

    if(attrsize!=NULL)
        *attrsize = attrvalue_binary_size(attrs[i].type,attrs[i].size_used);
    if(attrType!=NULL)
        *attrType = attrs[i].type;
    return loc;;
}

int tb_record_binary_length_set(Table* tb)
{
    int length = record_binary_lenth_predict(tb);
    tb->recordbinary_length = length;
    return length;
}

char** split(char* str, const char* spl, unsigned int max_segnum, unsigned int* n)
{
    char** dist = (char**)calloc(max_segnum*sizeof(char*),1);
    int cnt = 0;
    char* result = strtok(str, spl);
    char* onestr;
    while( result != NULL && cnt<max_segnum)
    {
        onestr= (char*)malloc(strlen(result)+1);
        strcpy(onestr, result);
        dist[cnt++] = onestr;
        result = strtok(NULL, spl);
    }

    if(n != NULL)
        *n = cnt;
    return dist;
}

void freeStringList(char** list,int stringNum)
{
    int i;
    for(i=0; i<stringNum; i++)
    {
        free(*(list+i));
    }
    free(list);
}


int CreateTable(char* tbname,int attrnum,attrinfo* attrs,char* errmsg)
{
    Table tb;
    tb.belongDB = "newdb";
    tb.tb_name = tbname;
    tb.attr_num = attrnum;
    tb.attrs = attrs;

    tb.primary_num = 0;
    tb.index_num = 0;

    return tb_create(&tb);
}

int DropTable(char* tbname,char* errmsg)
{
    char metaPath[_MAXLEN_TABLEPATH];
    char dataPath[_MAXLEN_TABLEPATH];
    tb_metafile_path(metaPath,"newdb",tbname);
    tb_datafile_path(dataPath,"newdb",tbname);

    _delTableFlag_("newdb",tbname);/***********/

    int rnt;
    rnt = remove(metaPath);
    if(rnt<0)   return rnt;
    return remove(dataPath);
}
