
/* persistly insert a record binary into table without checking the legality
   it may modify other recocrds content. e.g. the overflow area.
   */
//int record_binary_insert_persist(char* record_binary,size_t size,FILE *tablefile)
//{
//    //appenf
//    record_binary_append_persist(record_binary,size,tablefile);
//    //modify
//}


//int record_persist(RecordNode* recordnode,Table* tb,FILE* fp)
//{
//    RecordNode* node = recordnode;
//    enum RecordOP op = recordnode->optype;
//    switch(op)
//    {
//    case OP_I:
//        record_binary_append_persist(node->recordbinary,tb->recordbinary_length,fp);
//        break;
//    case OP_R:
//        record_binary_modify_persist(flag_INVAL,1,node->paddress,fp);
//        break;
//    case OP_M:
//        record_binary_modify_persist(node->recordbinary,tb->recordbinary_length,node->paddress,fp);
//        break;
//    case OP_S:
//        break;
//    default:
//        return -1;
//    }
//    return 0;
//}
