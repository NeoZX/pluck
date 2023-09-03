#include <stdio.h>
#include <stddef.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#include <sys/stat.h>

#include <linux/falloc.h>
#include <stdlib.h>

#include "fb_struct.h"
#include "commit.h"

//Error code
#define ERR_IO 1
#define ERR_UNSUPPORTED_ODS 2
#define ERR_TRIM 3
#define ERR_DB_NOT_LOCKED 4
#define ERR_DB_ENCRYPTED 5

#define MAX_THREADS 128

#define MAX_SUPPORTED_ODS 6
const unsigned short supported_ods[MAX_SUPPORTED_ODS] = {
        //0x000A, //Firebird 1.X
        0x800B, //Firebird 2.X
        0x800C, //Firebird 3.X
        0x800D, //Firebird 4.X
        0xE002, //RedDatabase 2.X
        0xE00C, //RedDatabase 3.X
        0xE00D,  //RedDatabase 4.X
};
const char *supported_db[MAX_SUPPORTED_ODS] = {
        "Firebird 2.X",
        "Firebird 3.X",
        "Firebird 4.X",
        "RedDatabase 2.X",
        "RedDatabase 3.X",
        "RedDatabase 4.X",
};

short goodbye = 0;
#define DEFAULT_BLOCK_SIZE 4096
short block_size = DEFAULT_BLOCK_SIZE;
#define DEFAULT_PROGRESS_BAR_STEP 16777216
int progress_bar_step = 0;
short trim = 0; //Default dry-run
short stage = 2;
int threads_count = 1;
short log_level = 1;
char *db_filename;
int fd;
long total_pages;
struct header_page header_page;
long pages_for_trim = 0;
long blocks_for_trim = 0;
struct stage2_info {
    pthread_t thread_id;
    long thread_number;
    int status;
    long start;
    long finish;
    long blocks_for_trim;
};

int is_supported_ods() {
    for (short i = 0; i < MAX_SUPPORTED_ODS; i++) {
        if (header_page.hdr_ods_version == supported_ods[i])
            return 1;
    }
    return 0;
}

const char *ods2str() {
    for (short i = 0; i < MAX_SUPPORTED_ODS; i++) {
        if (header_page.hdr_ods_version == supported_ods[i]) {
            return supported_db[i];
        }
    }
    return "\0";
}

int byte2str (char *string, long size) {
    if (size < 10240) {
        sprintf(string, "%ld B", size);
        return 0;
    }
    if (size < 10485760) {
        sprintf(string, "%ld KiB", size / 1024);
        return 0;
    }
    if (size < 10737418240) {
        sprintf(string, "%ld MiB", size / 1048576);
        return 0;
    }
    if (size < 10995116277760) {
        sprintf(string, "%ld GiB", size / 1073741824);
        return 0;
    }
    sprintf(string, "%ld TiB", size / 1099511627776);
    return 0;
}

void help(char *name) {
    printf("Usage %s [options]\n"
           "Default dry-run\n"
           "Available options:\n"
           "\t-h help\n"
           "\t-v version\n"
           "\t-b block size 512 or 4096, default 4096\n"
           "\t-t trim, default off\n"
           "\t-s stage 1 or 2, default 2\n"
           "\t\tstage 1 - search for free page using PIP\n"
           "\t\tstage 2 - stage 1, then search for unused blocks on pages\n"
           "\t-p parallel threads on stage 2, between 1 and %d\n"
           "\t-P print progress bar, step 16 MiB\n"
           "\t-d log level 0-3, default 1\n"
           "\t-f database.fdb\n", name, MAX_THREADS);
}

void version(char *name) {
    printf("%s version " pluck_VERSION " " COMMIT_HASH "\n", name);
    printf("Supported ODS:\n");
    for (short i = 0; i < MAX_SUPPORTED_ODS; i++) {
        printf("\tODS 0x%X - %s\n", supported_ods[i], supported_db[i]);
    }
}

int parse(int argc, char *argv[]) {
    char *opts = "hvtb:d:f:s:p:P";
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
            case 'p':
                threads_count = atoi(optarg);
                if ((threads_count < 1) || (threads_count > MAX_THREADS)){
                    printf("Threads count must be between 1 and %d\n", MAX_THREADS);
                    goodbye = 2;
                }
                break;
            case 'P':
                progress_bar_step = DEFAULT_PROGRESS_BAR_STEP;
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

int stage1(void)
{
    struct page_header *page;
    struct pip {
        UCHAR bits[1];
    };
    struct pip *pip;
    ULONG *pip_min;
    //struct pip_page_ods11 *pip_page;
    const USHORT page_size = header_page.hdr_page_size;
    const USHORT ods_version = header_page.hdr_ods_version;
    char message[128];

    //read first pip page
    short pip_num = 1;
    ULONG pages_in_pip;
    page = malloc(page_size);
    switch (ods_version) {
        case 0x800B: //Firebird 2.X
        case 0xE002: //RedDatabase 2.X
            pip = (struct pip *) page + offsetof(struct pip_page_ods11, bits);
            pip_min = (ULONG *) page + offsetof(struct pip_page_ods11, min);
            pages_in_pip = (page_size - offsetof(struct pip_page_ods11, bits)) * 8;
            break;
        case 0x800C: //Firebird 3.X
        case 0xE00C: //RedDatabase 3.X
        case 0x800D: //Firebird 4.X
        case 0xE00D: //RedDatabase 4.X
            pip = (struct pip *) page + offsetof(struct pip_page_ods12, bits);
            pip_min = (ULONG *) page + offsetof(struct pip_page_ods12, min);
            pages_in_pip = (page_size - offsetof(struct pip_page_ods12, bits)) * 8;
            break;
        default:
            return ERR_UNSUPPORTED_ODS;
            break;
    }
    sprintf(message, "Read 1 pip page %u\n", FIRST_PIP_PAGE);
    mylog(2, message);
    if (pread(fd, page, page_size, FIRST_PIP_PAGE * page_size) != page_size) {
        fprintf(stderr, "Error read page %u\n", FIRST_PIP_PAGE);
        free(page);
        return ERR_IO;
    }

    for (long i = *pip_min; i < total_pages; i++) {
        //Read next pip?
        if (i + 1 == pip_num * pages_in_pip) {
            sprintf(message, "Read %d pip page %lu\n", pip_num + 1, i);
            mylog(2, message);
            if (pread(fd, page, page_size, i * page_size) != page_size) {
                fprintf(stderr, "Error read page %lu\n", i);
                free(page);
                return ERR_IO;
            }
            if (page->page_type != PT_PAGE_INVENTORY) {
                fprintf(stderr, "Page %lu is not pip!\n", i);
                free(page);
                return ERR_IO;
            }
            pip_num++;
        } else {
            //Stage 1: check page in PIP
            if ((pip->bits[i % pages_in_pip / 8]) & (0x01 << i % 8)) {
                pages_for_trim++;
                //trim
                sprintf(message, "trim page %lu\n", i + 1);
                mylog(3, message);
                if (trim) {
                    if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, i * page_size, page_size)) {
                        fprintf(stderr, "fallocate failed\n");
                        free(page);
                        return ERR_TRIM;
                    }
                }
            }
        }
        //Print status bar
        if (progress_bar_step > 0 && (((i * page_size) % progress_bar_step == 0) || (i + 1 == total_pages))) {
            char db_size[32];
            byte2str(db_size, total_pages * page_size);
            char processed_size[32];
            byte2str(processed_size, (i + 1) * page_size);
            fprintf(stdout, "\rProcessed bytes %s / %s (%ld%%)",
                    processed_size, db_size, 100 * (i + 1) / total_pages);
            fflush(stdout);
        }
    }
    if (progress_bar_step > 0){
        fprintf(stdout, "\n");
        fflush(stdout);
    }
    free(page);
    return 0;
}

void * stage2(void *argv) {
    struct stage2_info *arg;
    arg = argv;

    char *page;
    unsigned long page_bitmap;  //MAX_PAGE_SIZE / min(block_size) = 32768 / 512 = 64 bit
    struct page_header *page_header;
    struct data_page *data_page;
    struct blob_page *blob_page;
    struct btree_page *btree_page;
    unsigned long page_bitmap_fill = -1;
    unsigned long data_page_bitmap_fill;
    unsigned long blob_page_bitmap_fill;
    unsigned long btree_page_bitmap_fill;
    const USHORT page_size = header_page.hdr_page_size;
    const USHORT ods_version = header_page.hdr_ods_version;
    char message[128];
    long blocks_for_trim_thr = 0;

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
    for (int bit = 0; bit <= (sizeof(struct btree_page)) / block_size; bit++) {
        btree_page_bitmap_fill |= 1UL << bit;
    }

    page = malloc(page_size);
    for (long i = arg->start; i < arg->finish; i++) {
        //Stage 2: Analyze page filling
        if (pread(fd, page, page_size, i * page_size) == page_size) {
            page_header = (struct page_header *) page;
            page_bitmap = 0;
            switch (page_header->page_type) {
                case PT_UNDEFINED_PAGE:
                    page_bitmap = 0UL;
                    break;
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
                    btree_page = (struct btree_page *) page;
                    if (btree_page->btr_length >= page_size - block_size)
                    {
                        page_bitmap = page_bitmap_fill;
                    } else {
                        page_bitmap = btree_page_bitmap_fill;
                        const int low_bit = (int) (sizeof(struct btree_page)) / block_size;
                        const int high_bit = (int) (btree_page->btr_length) / block_size;
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
                        blocks_for_trim_thr++;
                        if (trim) {
                            if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                                          i * page_size + bit * block_size, block_size)) {
                                fprintf(stderr, "fallocate failed\n");
                                free(page);
                                arg->blocks_for_trim = blocks_for_trim_thr;
                                arg->status = ERR_TRIM;
                                return 0;
                            }
                        }
                    }
                }
            }
        } else {
            free(page);
            arg->blocks_for_trim = blocks_for_trim_thr;
            arg->status = ERR_IO;
            return 0;
        }
        //Print status bar
        if ((progress_bar_step > 0) &&  ((((i - arg->start ) * page_size)  % progress_bar_step == 0) || (i + 1 == arg->finish))) {
            char buf[MAX_THREADS + 1];
            buf[0] = '\r';
            for(int t = 0; t < arg->thread_number; t++) {
                buf[t + 1] = '\t';
            }
            // proc
            fprintf(stdout, "%s%ld%%", buf, 100 * (i + 1 - arg->start) / (arg->finish - arg->start));
            fflush(stdout);
        }
    }
    free(page);
    arg->blocks_for_trim = blocks_for_trim_thr;
    arg->status = 0;
    return 0;
}

int main(int argc, char *argv[]) {
    struct stat fstat_before, fstat_after;
    int status;
    char message[128];
    int err = 0;
    char buf4size[32];

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
    if (pread(fd, &header_page, sizeof(header_page), 0) != sizeof(header_page))
    {
        fprintf(stderr, "Error reading header page\n");
        return ERR_IO;
    }
    sprintf(message, "Page size %d\n", header_page.hdr_page_size);
    mylog(1, message);
    sprintf(message, "ODS version %x (%s)\n", header_page.hdr_ods_version, ods2str());
    mylog(1, message);

    //CHECKS

    //Check argument compatibility
    //Ключи -d и -P не совместимы
    if (progress_bar_step > 0 && log_level > 1) {
        fprintf(stderr, "Progress bar and debug level greater than 1 are incompatible.\n");
        return 1;
    }

    //Check ODS version
    if (!is_supported_ods()) {
        fprintf(stderr, "Unsupported ODS version\n");
        return ERR_UNSUPPORTED_ODS;
    }

    //Check block_size and page_size
    if (block_size > header_page.hdr_page_size) {
        fprintf(stderr, "block size (%d) is large that page size (%d)\n", block_size, header_page.hdr_page_size);
        return 1;
    }
    if ((block_size == header_page.hdr_page_size) && (stage == 2)) {
        sprintf(message, "block size (%d) is equal page size (%d), set stage = 1\n", block_size, header_page.hdr_page_size);
        mylog(1, message);
    }

    //checks with database header flags
    USHORT hdr_flags;
    if (pread(fd, &hdr_flags, sizeof(hdr_flags), offsetof(struct header_page, hdr_flags)) != sizeof(hdr_flags)) {
        fprintf(stderr, "Error read hdr_flags\n");
        return ERR_IO;
    }

    //check encrypted database Red Database 2.6
    if (header_page.hdr_ods_version == 0xE002) {
        if (hdr_flags & (rdb26_hdr_encrypted | hdr_crypt_process)) {
            fprintf(stderr, "Database is encrypted or is currently encrypting.\n");
            return ERR_DB_ENCRYPTED;
        }
    }
    //check encrypted database Firebird 3/4, Red Database 3/4
    if ((header_page.hdr_ods_version == 0x800C) || (header_page.hdr_ods_version == 0x800D) || (header_page.hdr_ods_version == 0xE00C) || (header_page.hdr_ods_version == 0xE00D)) {
        if ((stage == 2) && (hdr_flags & (hdr_crypt_process | hdr_encrypted))) {
            fprintf(stderr, "Database is encrypted or is currently encrypting. Set stage = 1\n");
            stage = 1;
        }
    }

    //check shutdown or nbackup lock
    if (trim) {
        //Check database full shutdown or nbackup lock
        if (((hdr_flags & hdr_shutdown_mask) != hdr_shutdown_full) &&
            ((hdr_flags & hdr_backup_mask) != hdr_nbak_stalled)) {
            fprintf(stderr, "Database must be full shutdown or nbackup lock\n");
            return ERR_DB_NOT_LOCKED;
        }
    }

    stat(db_filename, &fstat_before);
    total_pages = fstat_before.st_size / header_page.hdr_page_size;

    //Stage 1
    status = stage1();
    if (status != 0) {
        close(fd);
        return status;
    }

    byte2str(buf4size, pages_for_trim * header_page.hdr_page_size);
    sprintf(message, "Stage 1: Pages for trim %ld (%s)\n", pages_for_trim, buf4size);
    mylog(1, message);

    //Stage 2
    if (stage == 2) {
        struct stage2_info stage2_info[MAX_THREADS];
        if (progress_bar_step > 0) {
            for (long thread = 0; thread < threads_count; thread++) {
                //Progress bar
                fprintf(stdout, "Thr %ld\t", thread);
            }
            fprintf(stdout, "\n");
            fsync(STDOUT_FILENO);
        }
        for (long thread = 0; thread < threads_count; thread++) {
            stage2_info[thread].thread_number = thread;
            stage2_info[thread].start = total_pages * (thread) / threads_count;
            stage2_info[thread].finish = total_pages * (thread + 1) / threads_count;
            sprintf(message, "Starting thread %ld range %ld - %ld\n",
                    thread, stage2_info[thread].start, stage2_info[thread].finish);
            mylog(2, message);
            pthread_create(&(stage2_info[thread].thread_id), NULL, stage2, &stage2_info[thread]);
        }
        for (long thread = 0; thread < threads_count; thread++) {
            pthread_join(stage2_info[thread].thread_id, NULL);
            blocks_for_trim += stage2_info[thread].blocks_for_trim;
            if (stage2_info[thread].status > 0) {
                fprintf(stderr, "Error %d on thread %ld\n", stage2_info[thread].status, thread);
                err = stage2_info[thread].status;
            }
        }
        if (progress_bar_step > 0) {
            fprintf(stdout, "\n");
        }
    }

    close(fd);
    if (stage == 2) {
        byte2str(buf4size, blocks_for_trim * block_size);
        sprintf(message, "Stage 2: Blocks for trim %ld (%s)\n", blocks_for_trim, buf4size);
        mylog(1, message);
    }
    stat(db_filename, &fstat_after);
    if (trim) {
        const long file_size_reduced = (fstat_before.st_blocks - fstat_after.st_blocks) * fstat_after.st_blksize;
        byte2str(buf4size, file_size_reduced);
        sprintf(message, "FS block usage reduced from %ld to %ld (FS block size %ld)\nReleased %s\n",
                fstat_before.st_blocks, fstat_after.st_blocks, fstat_after.st_blksize, buf4size);
        mylog(2, message);
    }
    return err;
}
