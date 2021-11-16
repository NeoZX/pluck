#include <stdio.h>
#include <stddef.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <sys/stat.h>
#include <math.h>

#include <linux/falloc.h>
#include <stdlib.h>

#include "fb_struct.h"

#define PLUCK_VERSION "0.1"

//Error code
#define ERR_IO 1
#define ERR_UNSUPPORTED_ODS 2
#define ERR_TRIM 3
#define ERR_DB_NOT_LOCKED 4

#define MAX_SUPPORTED_ODS 2
const unsigned short supported_ods[MAX_SUPPORTED_ODS] = {
        //0x000A, //Firebird 1.X
        0x800B, //Firebird 2.X
        //0x800C, //Firebird 3.X
        //0x800D, //Firebird 4.X
        0xE002, //RedDatabase 2.X
        //0xE00C, //RedDatabase 3.X
        //0xE00D,  //RedDatabase 4.X
};
const char *supported_db[MAX_SUPPORTED_ODS] = {
        "Firebird 2.X",
        //"Firebird 3.X",
        //"Firebird 4.X",
        "RedDatabase 2.X",
        //"RedDatabase 3.X",
        //"RedDatabase 4.X",
};

short goodbye = 0;
#define DEFAULT_BLOCK_SIZE 4096
short block_size = DEFAULT_BLOCK_SIZE;
short trim = 0; //Default dry-run
short stage = 2;
short log_level = 1;
char *db_filename;

int is_supported_ods(unsigned short ods_version) {
    for (short i = 0; i < MAX_SUPPORTED_ODS; i++) {
        if (ods_version == supported_ods[i])
            return 1;
    }
    return 0;
}

char *ods2str(unsigned short ods_version) {
    char *db_version = "\0";
    for (short i = 0; i < MAX_SUPPORTED_ODS; i++) {
        if (ods_version == supported_ods[i]) {
            db_version = supported_db[i];
        }
    }
    return db_version;
}

void help(char *name) {
    printf("Usage %s [options]\n"
           "Default dry-run\n"
           "Available options:\n"
           "\t-h help\n"
           "\t-v version\n"
           "\t-b block size 512 or 4096, default 4096\n"
           "\t-t trim\n"
           "\t-s stage 1 or 2, default 2\n"
           "\t\tstage 1 - search for free page using PIP\n"
           "\t\tstage 2 - stage 1, then search for unused blocks on pages\n"
           "\t-d log level\n"
           "\t-f database.fdb\n", name);
}

void version(char *name) {
    printf("%s version " PLUCK_VERSION "\n", name);
    printf("Supported ODS:\n");
    for (short i = 0; i < MAX_SUPPORTED_ODS; i++) {
        printf("\tODS 0x%X - %s\n", supported_ods[i], supported_db[i]);
    }
}

int parse(int argc, char *argv[]) {
    char *opts = "hvtb:d:f:s:";
    int opt;
    while ((opt = getopt(argc, argv, opts)) != -1) {
        switch (opt) {
            case 'h':
                help(argv[0]);
                goodbye = 1;
                break;
            case 'v':
                version(argv[0]);
                goodbye = 1;
                break;
            case 'b':
                //todo: change atoi to strtol
                block_size = (short) atoi(optarg);
                if ((block_size != 512) && (block_size != 4096)) {
                    printf("Wrong block size %s\n", optarg);
                    goodbye = 2;
                }
                break;
            case 't':
                trim = 1;
                break;
            case 'd':
                log_level = (short) atoi(optarg);
                break;
            case 'f':
                db_filename = optarg;
                break;
            case 's':
                stage = (short) atoi(optarg);
                if ((stage != 1) && (stage != 2)) {
                    printf("Stage must be 1 or 2, not %d\n", stage);
                    goodbye = 2;
                }
                break;
            default:
                fprintf(stderr, "Unknown argument %s\n", optarg);
        }
    }
    return 0;
}

int mylog(int level, char *message) {
    if (log_level >= level) {
        fprintf(stdout, "%s", message);
    }
    return 0;
}

int main(int argc, char *argv[]) {
    USHORT page_size;
    USHORT ods_version;
    struct page_header *page_header;
    struct pip_page_ods11 *pip_page;
    struct data_page *data_page;
    struct blob_page *blob_page;
    struct btree_page_ods11 *btree_page;
    struct stat fstat_before, fstat_after;
    char *page;
    unsigned long page_bitmap;  //MAX_PAGE_SIZE / min(block_size) = 32768 / 512 = 64 bit
    unsigned long page_bitmap_fill = -1;
    unsigned long data_page_bitmap_fill;
    unsigned long blob_page_bitmap_fill;
    unsigned long btree_page_bitmap_fill;
    long pages_for_trim = 0;
    long blocks_for_trim = 0;
    char message[128];

    parse(argc, argv);
    if (goodbye > 0)
        return goodbye - 1;
    if (!db_filename) {
        help(argv[0]);
        return 0;
    }

    if (!trim) {
        mylog(1, "Dry run mode\n");
    }

    int fd;
    if (trim) {
        fd = open(db_filename, O_RDWR);
    } else {
        fd = open(db_filename, O_RDONLY);
    }
    if (fd < 0) {
        fprintf(stderr, "Error %d open file %s\n", fd, db_filename);
        return ERR_IO;
    }

    //read page_size & ods_version
    pread(fd, &page_size, sizeof(page_size), offsetof(struct header_page, hdr_page_size));
    pread(fd, &ods_version, sizeof(ods_version), offsetof(struct header_page, hdr_ods_version));
    sprintf(message, "Page size %d\n", page_size);
    mylog(1, message);
    sprintf(message, "ODS version %x (%s)\n", ods_version, ods2str(ods_version));
    mylog(1, message);

    page = malloc(page_size);
    page_header = (struct page_header *) page;
    page_bitmap_fill = page_bitmap_fill >> (8 * sizeof(page_bitmap_fill) - page_size / block_size);
    data_page_bitmap_fill = 0;
    for (int bit = 0; bit <= (offsetof(struct data_page, dpg_rpt)) / block_size; bit++) {
        data_page_bitmap_fill |= 1UL << bit;
    }
    blob_page_bitmap_fill = 0;
    for (int bit = 0; bit <= (offsetof(struct blob_page, blp_page)) / block_size; bit++) {
        blob_page_bitmap_fill |= 1UL << bit;
    }
    btree_page_bitmap_fill = 0;
    for (int bit = 0; bit <= (offsetof(struct btree_page_ods11, btr_nodes)) / block_size; bit++) {
        btree_page_bitmap_fill |= 1UL << bit;
    }

    if (trim) {
        USHORT hdr_flags;
        if (pread(fd, &hdr_flags, sizeof(hdr_flags), offsetof(struct header_page, hdr_flags)) == sizeof(hdr_flags)) {
            //Check database full shutdown or nbackup lock
            if (((hdr_flags & hdr_shutdown_mask) != hdr_shutdown_full) && ((hdr_flags & hdr_backup_mask) != hdr_nbak_stalled)) {
                fprintf(stderr, "Database must be full shutdown or nbackup lock\n");
                return ERR_DB_NOT_LOCKED;
            }
        } else {
            fprintf(stderr, "Error read hdr_flags\n");
            return ERR_IO;
        }
    }

    if (page_size <= block_size) {
        fprintf(stderr, "Page size (%d) less or equal block size (%d)\n", page_size, block_size);
        return ERR_IO;
    }
    if (!is_supported_ods(ods_version)) {
        fprintf(stderr, "Unsupported ODS version\n");
        return ERR_UNSUPPORTED_ODS;
    }

    //read first pip page
    short pip_num = 1;
    unsigned int pages_in_pip;
    pages_in_pip = (page_size - sizeof(pip_page->header) - sizeof(pip_page->min)) * 8;
    pip_page = malloc(page_size);
    pread(fd, pip_page, page_size, FIRST_PIP_PAGE * page_size);

    long i = 0;
    stat(db_filename, &fstat_before);
    long total_pages = fstat_before.st_size / page_size;

    for (i = 2; i < total_pages; i++) {
        //Read next pip?
        if (i + 1 == pip_num * pages_in_pip) {
            sprintf(message, "Read %d pip page %lu\n", pip_num + 1, i);
            mylog(2, message);
            if (pread(fd, pip_page, page_size, i * page_size) != page_size) {
                fprintf(stderr, "Error read page %lu\n", i);
                return ERR_IO;
            }
            if (pip_page->header.page_type != PT_PAGE_INVENTORY) {
                fprintf(stderr, "Page %lu is not pip!\n", i);
                return ERR_IO;
            }
            pip_num++;
        } else {
            //Stage 1: check page in PIP
            if ((pip_page->bits[i % pages_in_pip / 8]) & (0x01 << i % 8)) {
                pages_for_trim++;
                //trim
                sprintf(message, "trim page %lu\n", i + 1);
                mylog(3, message);
                if (trim) {
                    if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, i * page_size, page_size)) {
                        fprintf(stderr, "fallocate failed\n");
                        return ERR_TRIM;
                    }
                }
            } else {
                //Stage 2: Analyze page filling
                if (stage == 2) {
                    if (pread(fd, page, page_size, i * page_size) == page_size) {
                        page_header = (struct page_header *) page;
                        page_bitmap = 0;
                        switch (page_header->page_type) {
                            case PT_DATA:
                                data_page = (struct data_page *) page;
                                page_bitmap = data_page_bitmap_fill;
                                for (int bit = (int) (offsetof(struct data_page, dpg_rpt)) / block_size;
                                     bit <= (offsetof(struct data_page, dpg_rpt) +
                                             sizeof(struct dpg_repeat) * data_page->count) / block_size;
                                     bit++)
                                {
                                    page_bitmap |= 1UL << bit;
                                }
                                for (unsigned short cnt = 0; cnt < data_page->count; cnt++) {
                                    const int low_bit = data_page->dpg_rpt[cnt].dpg_offset / block_size;
                                    const int high_bit = (data_page->dpg_rpt[cnt].dpg_offset +
                                                          data_page->dpg_rpt[cnt].dpg_length - 1) / block_size;
                                    for (int bit = low_bit; bit <= high_bit; bit++) {
                                        page_bitmap |= 1UL << bit;
                                    }
                                }
                                break;
                            case PT_B_TREE:
                                btree_page = (struct btree_page_ods11 *) page;
                                if (offsetof(struct btree_page_ods11, btr_nodes) + btree_page->btr_length
                                        >= page_size - block_size)
                                {
                                    page_bitmap = page_bitmap_fill;
                                } else {
                                    page_bitmap = btree_page_bitmap_fill;
                                    const int low_bit =
                                            (int) (offsetof(struct btree_page_ods11, btr_nodes)) / block_size;
                                    const int high_bit = (int) (offsetof(struct btree_page_ods11, btr_nodes)
                                            + btree_page->btr_length) / block_size;
                                    for (int bit = low_bit; bit <= high_bit; bit++) {
                                        page_bitmap |= 1UL << bit;
                                    }
                                }
                                break;
                            case PT_BLOB:
                                blob_page = (struct blob_page *) page;
                                if (offsetof(struct blob_page, blp_page) + blob_page->blp_length >= page_size - block_size) {
                                    page_bitmap = page_bitmap_fill;
                                } else {
                                    page_bitmap = blob_page_bitmap_fill;
                                    const int low_bit = (int) (offsetof(struct blob_page, blp_page)) / block_size;
                                    const int high_bit =
                                            (int) (offsetof(struct blob_page, blp_page) + blob_page->blp_length) / block_size;
                                    for (int bit = low_bit; bit <= high_bit; bit++) {
                                        page_bitmap |= 1UL << bit;
                                    }
                                }
                                break;
                        }

                        //Trim blocks
                        if ((page_bitmap < page_bitmap_fill) && (page_bitmap != 0)) {
                            sprintf(message, "Page %lu (%s), bitmap 0x%lx\n", i,
                                    page_type_name[page_header->page_type], page_bitmap);
                            mylog(3, message);
                            //blocks for trim
                            for (int bit = 0; bit < page_size / block_size; bit++) {
                                if (!(page_bitmap & (1UL << bit))) {
                                    blocks_for_trim++;
                                    if (trim) {
                                        if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                                                      i * page_size + bit * block_size, block_size)) {
                                            fprintf(stderr, "fallocate failed\n");
                                            return ERR_TRIM;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        free(page);
                        return ERR_IO;
                    }
                }
            }
        }
    }
    free(page);
    free(pip_page);
    close(fd);
    sprintf(message, "Stage 1: Pages for trim %ld (%ld bytes)\n", pages_for_trim, pages_for_trim * page_size);
    mylog(1, message);
    if (stage == 2) {
        sprintf(message, "Stage 2: Blocks for trim %ld (%ld bytes)\n", blocks_for_trim, blocks_for_trim * block_size);
        mylog(1, message);
    }
    stat(db_filename, &fstat_after);
    const long file_size_reduced = (fstat_before.st_blocks - fstat_after.st_blocks) * fstat_after.st_blksize;
    sprintf(message, "FS block usage reduced from %ld to %ld (FS block size %ld)\nReleased %ld bytes",
            fstat_before.st_blocks, fstat_after.st_blocks, fstat_after.st_blksize, file_size_reduced);
    mylog(2, message);
    return 0;
}
