//
// Created by volkov on 04.11.2021.
//

#ifndef FB_TRIM_FB_STRUCT_H
#define FB_TRIM_FB_STRUCT_H

#define SCHAR signed char
#define UCHAR unsigned char
#define SSHORT signed short
#define USHORT unsigned short
#define SLONG signed int
#define ULONG unsigned int

struct page_header
{
    SCHAR page_type;
    UCHAR page_flags;
    USHORT pag_checksum;
    ULONG pag_generation;
    ULONG pag_scn;
    ULONG reserved;
};

struct data_page
{
    SLONG dpg_sequence;
    USHORT relation;
    USHORT count;
};
struct dpg_repeat
{
    USHORT dpg_offset;
    USHORT dpg_length;
};

struct pip_page
{
    struct page_header header;
    SLONG min;
    UCHAR bits[1];
};

#define PT_UNDEFINED_PAGE 0
#define PT_HEADER 1
#define PT_PAGE_INVENTORY 2
#define PT_TRANSACTION_INVENTORY 3
#define PT_POINTER_PAGE 4
#define PT_DATA 5
#define PT_INDEX_ROOT 6
#define PT_B_TREE 7
#define PT_BLOB 8
#define PT_GENERATOR 9
#define PT_WRITE_AHEAD_LOG 10
#define MAX_PAGE_TYPE 11
const char* page_type_name[MAX_PAGE_TYPE] = {
        "undefined",
        "header page",
        "page inventory page",
        "transaction inventory page",
        "pointer page",
        "data page",
        "index root page",
        "index b-tree page",
        "blob page",
        "generator page",
        "write ahead log"
};

#endif //FB_TRIM_FB_STRUCT_H


