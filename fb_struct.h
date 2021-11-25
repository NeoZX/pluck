//
// Created by volkov on 04.11.2021.
//

#ifndef FB_TRIM_FB_STRUCT_H
#define FB_TRIM_FB_STRUCT_H

typedef signed char SCHAR;
typedef unsigned char UCHAR;
typedef signed short SSHORT;
typedef unsigned short USHORT;
typedef signed int SLONG;
typedef unsigned int ULONG;

struct page_header {
    SCHAR page_type;
    UCHAR page_flags;
    USHORT pag_checksum;
    ULONG pag_generation;
    ULONG pag_scn;
    ULONG reserved;
};

//Universal header_page (cut) for ODS11, ODS12, ODS13
struct header_page
{
    struct page_header header;
    USHORT hdr_page_size;           // Page size of database
    USHORT hdr_ods_version;         // Version of on-disk structure
    SLONG hdr_PAGES;                // Page number of PAGES relation
    ULONG hdr_next_page;            // Page number of next hdr page
    SLONG hdr_oldest_transaction;   // Oldest interesting transaction
    SLONG hdr_oldest_active;        // Oldest transaction thought active
    SLONG hdr_next_transaction;     // Next transaction id
    USHORT hdr_sequence;            // sequence number of file
    USHORT hdr_flags;               // Flag settings, see below
    SLONG hdr_creation_date[2];     // Date/time of creation
    SLONG hdr_attachment_id;        // Next attachment id
    SLONG hdr_shadow_count;         // Event count for shadow synchronization
};

// Header page flags

const USHORT hdr_active_shadow          = 0x1;          // 1    file is an active shadow file
const USHORT hdr_force_write            = 0x2;          // 2    database is forced write
const USHORT hdr_crypt_process          = 0x4;          // 4    Encryption status is changing now
const USHORT hdr_encrypted              = 0x8;          // 8    Database is encrypted
const USHORT hdr_no_checksums           = 0x10;         // 16   don't calculate checksums
const USHORT hdr_no_reserve             = 0x20;         // 32   don't reserve space for versions
const USHORT hdr_replica                = 0x40;         // 64   database is a replication target
const USHORT hdr_SQL_dialect_3          = 0x100;        // 256  database SQL dialect 3
const USHORT hdr_read_only              = 0x200;        // 512  Database in ReadOnly. If not set, DB is RW
const USHORT hdr_backup_mask            = 0xC00;
const USHORT hdr_shutdown_mask          = 0x1080;

// Values for backup mask
const USHORT hdr_nbak_normal		= 0x000;	// Normal mode. Changes are simply written to main files
const USHORT hdr_nbak_stalled		= 0x400;	// 1024 Main files are locked. Changes are written to diff file
const USHORT hdr_nbak_merge			= 0x800;	// 2048 Merging changes from diff file into main files

// Values for shutdown mask
const USHORT hdr_shutdown_none          = 0x0;
const USHORT hdr_shutdown_multi         = 0x80;
const USHORT hdr_shutdown_full          = 0x1000;
const USHORT hdr_shutdown_single        = 0x1080;

//ODS11, ODS12, ODS13
struct blob_page {
    struct page_header header;
    ULONG blp_lead_page;        // First page of blob (for redundancy only)
    ULONG blp_sequence;         // Sequence within blob
    USHORT blp_length;          // Bytes on page
    USHORT blp_pad;             // Unused
    ULONG blp_page[1];          // Page number if level 1
};

//ODS11, ODS12, ODS13
struct data_page {
    struct page_header header;
    ULONG dpg_sequence;
    USHORT relation;
    USHORT count;
    struct dpg_repeat {
        USHORT dpg_offset;
        USHORT dpg_length;
    } dpg_rpt[1];
};

//ODS11
struct pip_page_ods11 {
    struct page_header header;
    SLONG min;
    UCHAR bits[1];
};

//ODS12, ODS13
struct pip_page_ods12 {
    struct page_header header;
    ULONG min;
    ULONG pip_extent;
    ULONG pip_used;
    UCHAR bits[1];
};

struct btree_page {
    struct page_header header;
    SLONG btr_sibling;			// right sibling page
    SLONG btr_left_sibling;		// left sibling page
    SLONG btr_prefix_total;		// sum of all prefixes on page
    USHORT btr_relation;		// relation id for consistency
    USHORT btr_length;			// length of data in bucket
    UCHAR btr_id;				// index id for consistency
    UCHAR btr_level;			// index level (0 = leaf)
};

const ULONG FIRST_PIP_PAGE = 1;

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
#define MAX_PAGE_TYPE 10

const char *page_type_name[MAX_PAGE_TYPE + 1] = {
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


