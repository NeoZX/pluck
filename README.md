# About the project
This utility is designed to reduce the size of the database without performing backup and restore operations. This may be necessary in some cases.


# Requirements
* glibс 2.18 (support for FALLOC_FL_ * flags)
* kernel 2.6.38 when using XFS
* kernel 3.0 when using Ext4

# Usage example
Runs in dry run mode by default:

    ./pluck -f database.fdb

Trim pages:

    ./pluck -t -f database.fdb

# Restrictions
Supported version Firebird 2.X, 3.X, 4.X, Red Database 2.X, 3.X
Supported block size 512 and 4096.
