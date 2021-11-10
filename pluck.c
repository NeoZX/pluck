#include <stdio.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <sys/stat.h>

#include <linux/falloc.h>
#include <stdlib.h>

#include "fb_struct.h"

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
const char* supported_db[MAX_SUPPORTED_ODS] = {
        "Firebird 2.X",
        //"Firebird 3.X",
        //"Firebird 4.X",
        "RedDatabase 2.X",
        //"RedDatabase 3.X",
        //"RedDatabase 4.X",
};

short goodbye = 0;
#define DEFAULT_BLOCK_SIZE 512
short block_size = DEFAULT_BLOCK_SIZE;
short trim = 0; //Default dry-run
short log_level = 1;
char *db_filename;

int is_supported_ods(unsigned short ods_version)
{
    for(short i = 0; i < MAX_SUPPORTED_ODS; i++)
    {
        if (ods_version == supported_ods[i])
            return 1;
    }
    return 0;
}

char *ods2str(unsigned short ods_version)
{
    char *db_version = "\0";
    for(short i = 0; i < MAX_SUPPORTED_ODS; i++)
    {
        if (ods_version == supported_ods[i])
        {
            db_version = supported_db[i];
        }
    }
    return db_version;
}

void help(char *name)
{
    printf("Usage %s [options]\n"
           "Default dry-run\n"
           "Available options:\n"
           "\t-h help\n"
           "\t-v version\n"
           "\t-b block size 512 or 4096, default 512\n"
           "\t-t trim\n"
           "\t-d log level\n"
           "\t-f database.fdb\n", name);
}

void version(char *name)
{
    printf("%s version 0.0\n", name);
    printf("Supported ODS:\n");
    for(short i = 0; i < MAX_SUPPORTED_ODS; i++)
    {
        printf("\tODS 0x%X - %s\n", supported_ods[i], supported_db[i]);
    }
}

int parse(int argc, char *argv[])
{
    char *opts = "hvtb:d:f:";
    int opt;
    while ((opt = getopt(argc, argv, opts)) != -1)
    {
        switch (opt)
        {
            case 'h':
                help(argv[0]);
                goodbye = 1;
                break;
            case 'v':
                version(argv[0]);
                goodbye = 1;
                break;
            case 'b':
                block_size = (short) atoi(optarg);
                if ((block_size != 512) && (block_size != 4096))
                {
                    fprintf(stderr, "Wrong block size %s\n", optarg);
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
            default:
                fprintf(stderr, "Unknown argument %s\n", optarg);
        }
    }
    return 0;
}

int mylog(int level, char *message)
{
    if (log_level >= level)
    {
        fprintf(stdout,"%s", message);
    }
    return 0;
}

int main(int argc, char *argv[]) {
    USHORT page_size;
    USHORT ods_version;
    struct page_header page_header;
    struct pip_page *pip_page;
    struct stat fstat;
    long pages_for_trim = 0;
    char message[128];

    if (argc == 1) {
        help(argv[0]);
        return 0;
    }
    parse(argc, argv);
    if (!db_filename)
    {
        help(argv[0]);
        return 0;
    }
    if (goodbye)
        return 0;

    if (!trim)
    {
        mylog(1, "Dry run mode\n");
    }

    int fd;
    if (trim)
    {
        fd = open(db_filename, O_RDWR);
    }
    else
    {
        fd = open(db_filename, O_RDONLY);
    }
    if (fd < 0)
    {
        fprintf(stderr, "Error %d open file %s\n", fd, db_filename);
        return ERR_IO;
    }

    //read page_size & ods_version
    lseek(fd, 0x10, SEEK_SET);
    read(fd, &page_size, sizeof(page_size));
    read(fd, &ods_version, sizeof(ods_version));
    sprintf(message, "Page size %d\n", page_size);
    mylog(1, message);
    sprintf(message, "ODS version %x (%s)\n", ods_version, ods2str(ods_version));
    mylog(1, message);

    if (trim)
    {
        USHORT hdr_flags;
        lseek(fd, 0x2a, SEEK_SET);
        if (read(fd, &hdr_flags, sizeof(hdr_flags)) == sizeof(hdr_flags))
        {
            //Check database full shutdown or nbackup lock
            if (((hdr_flags & 0x1080) != 0x1000) && ((hdr_flags & 0xC00) != 0x400))
            {
                fprintf(stderr, "Database must be full shutdown or nbackup lock\n");
                return ERR_DB_NOT_LOCKED;
            }
        }
        else
        {
            fprintf(stderr, "Error read hdr_flags\n");
            return ERR_IO;
        }
    }

    if (page_size <= block_size)
    {
        fprintf(stderr, "Page size (%d) less or equal block size (%d)\n", page_size, block_size);
        return ERR_IO;
    }
    if (! is_supported_ods(ods_version))
    {
        fprintf(stderr, "Unsupported ODS version\n");
        return ERR_UNSUPPORTED_ODS;
    }

    //read first pip page
    short pip_num = 1;
    unsigned int pages_in_pip;
    pages_in_pip = (page_size - sizeof(pip_page->header) - sizeof(pip_page->min)) * 8;
    pip_page = malloc(page_size);
    lseek(fd, 1 * page_size, SEEK_SET);
    read(fd, pip_page, page_size);

    long i = 0;
    stat(db_filename, &fstat);
    long total_pages = fstat.st_size / page_size;

    for(i = pip_page->min; i < total_pages; i++)
    {
        //Read next pip?
        if (i + 1 == pip_num * pages_in_pip)
        {
            sprintf(message, "Read %d pip page %lu\n", pip_num + 1, i);
            mylog(2, message);
            lseek(fd, i * page_size, SEEK_SET);
            if (read(fd, pip_page, page_size) != page_size)
            {
                fprintf(stderr, "Error read page %lu\n", i);
                return ERR_IO;
            }
            if (pip_page->header.page_type != PT_PAGE_INVENTORY)
            {
                fprintf(stderr, "Page %lu is not pip!\n", i);
                return ERR_IO;
            }
            pip_num++;
        }
        else
        {
            if ((pip_page->bits[i % pages_in_pip / 8]) & (0x01 << i % 8))
            {
                //Trim page only types 5,6,7,8 (date, i-root, b-tree, blob)
                lseek(fd, i * page_size, SEEK_SET);
                if (read(fd, &page_header, sizeof(page_header)) != sizeof(page_header))
                {
                    fprintf(stderr, "Error read at %lu\n", i * page_size);
                    return ERR_IO;
                }
                if ((page_header.page_type == PT_DATA)
                    || (page_header.page_type == PT_INDEX_ROOT)
                    || (page_header.page_type == PT_B_TREE)
                    || (page_header.page_type == PT_BLOB))
                {
                    pages_for_trim++;
                    //trim
                    sprintf(message, "trim page %lu\n", i + 1);
                    mylog(2, message);
                    if (trim)
                    {
                        if (fallocate(fd, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE, i * page_size + block_size, page_size - block_size))
                        {
                            fprintf(stderr, "fallocate failed\n");
                            return ERR_TRIM;
                        }
                    }
                }
            }
        }
    }
    free(pip_page);
    close(fd);
    sprintf(message, "Pages for trim %ld (%ld bytes)\n", pages_for_trim, pages_for_trim * (page_size - block_size));
    mylog(1, message);

    return 0;
}
