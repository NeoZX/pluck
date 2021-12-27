[EN](README.md) [RU](README.ru.md)

# Why you need it
This utility is designed to reduce the size of the database without performing backup and restore operations. This may be necessary in some cases.

# How it works
At the first stage, the utility searches for free pages according to the PIP data (Page Inventory Page). Then program frees filesystem blocks on free pages.

At the second stage, the utility analyzes the page usage. If there are unused blocks on the page, then those blocks are freed. Pages of the following types are analyzed: data, b-tree index, BLOB. These three types of pages make up >99% of the total number of pages in large databases.

When a block is freed at the file system level, the trim command is sent to the block device. You may also need to run the fstrim command. It depends on how the filesystem is mounted, see the discard option in the filesystem documentation.

It is not recommended to using it on HDD, as using the utility can lead to file fragmentation. On an SSD, file fragmentation has virtually no effect on read / write speed and access time.

#BEWARE, STRANGER!
Reducing the size of the database file can be useful in some cases. For example, if mostly read transactions are performed in database (in OLAP models, analytics, statistics, etc.). But sometime can be situations  when the file size will be larger than the file system size. So, this situation will arise when FS has not free blocks, but it has free pages in the file. This situation can lead to unexpected errors in the operation of the DBMS.

# Requirements
* glibс 2.18 (support for FALLOC_FL_* flags)
* kernel 2.6.38 when using XFS
* kernel 3.0 when using Ext4

For RHEL/CentOS 6 kernel >=2.6.32-358

# Usage example
Runs in dry run mode by default:

    ./pluck -f database.fdb

Trim blocks:

    ./pluck -t -f database.fdb

Trim blocks in 4 threads:

    ./pluck -p 4 -t -f database.fdb 

# Restrictions
* Multi-file databases are not supported.
* Supported ODS version 11, 12, 13 (Firebird 2/3/4).
* Encrypted files in Red Database 2.6 are not supported.
* Encrypted files in Firebird 3/4 are only supported at the first stage.
* Supported block size 512 and 4096.
* Supported OS Linux.

# Alternatives
Alternatively, you can use the system commands of the operating system, e.g. copy file with sparse option:

    cp --sparse=always original.fdb sparse.fdb

This will be less efficient, but it will also reduce file size.
