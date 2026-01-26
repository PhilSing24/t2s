# An Introduction to Compression in Q/KDB+


In the realm of computer science, compression refers to the process of reducing the size of a data object, such as a file, to save disk space and accelerate data transfer. In Q/KDB+, compression is both efficient and seamless: compressed data can be written to and read from disk without any additional effort on the user’s part. The language’s built-in methods for file handling automatically support both compressed and uncompressed formats, ensuring a smooth and transparent user experience.

This blog explores the fundamentals of compression in Q/KDB+, focusing on how to write and read compressed files. We will examine how compression operates for various data structures, such as tables and columns, and discuss how to apply compression settings effectively to optimise storage.

## Writing Compressed Files

In Q/KDB+, there are two main methods for writing compressed files:

Method 1: Write data to disk in an uncompressed form, then compress it later on disk.
Method 2: Write data directly to disk in compressed form.
Method 1 is beneficial when you want to bypass the overhead of compression during data saving, allowing it to be deferred until later when resources are less constrained. However, in most cases, it’s more efficient to compress the data and save it in a single step (Method 2).

Method 1: Write Uncompressed, Compress Later
To write data to disk in an uncompressed form, use the set keyword:

q)`:/path/to/file set data

Once saved, you can compress the file on disk using the set keyword again, but this time specifying compression parameters:

q)(targetFile;blockSize;algorithm;level) set srcFile
Where:

targetFile — The path to the compressed file.
blockSize — The logical block size, determining how much data to compress at a time.
algorithm — The compression algorithm to use.
level — The compression (zip) level.
srcFile — The path to the uncompressed source file.
Note: Performance depends heavily on the number of disk seeks. Placing the source and target files on separate physical disks can reduce seek time and improve performance.

Example

Let’s create a list of random data:

q)show data:1000?100 // List of 1000 random longs between 0 and 100
7 62 89 15 9 37 23..
Write the data to disk in uncompressed form:

q)`:data set data
`:data
Check the file’s size on disk:

C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     12:39           8016 data
Now, compress the uncompressed file:

// Compress using the q IPC compression algorithm
q)(`:compressedData1;16;1;0) set `:./data
`:compressedData1
Verify the compressed file size:

C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     12:51           2297 compressedData1
-a---l        02/10/2024     12:39           8016 data
As shown, the compressed file is much smaller than the uncompressed version.

Method 2: Write Compressed Data Directly
To compress and save data directly to disk in one step, use the set keyword with compression parameters:

q)(targetFile;blockSize;algorithm;level) set data
Where data is any Q object you wish to save in a compressed format.

Example

Using the same random data from the previous example, compress and save it directly:

q)(`:compressedData2;16;1;0) set data
`:compressedData2
Verify the compressed file size:

C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     12:51           2297 compressedData1
-a---l        02/10/2024     13:05           2297 compressedData2
-a---l        02/10/2024     12:49           8016 data
The compressed file compressedData2 is the same size as compressedData1, showing that the compression methods produce an equivalent result.

Appending to Compressed Data
It is also possible to append data to a compressed file using the upsert keyword. This allows you to add more data without needing to decompress the existing file:

q)`:compressedData2 upsert 1000?100 // Append another 1000 random longs
`:compressedData2
Check the file’s size after appending:

C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     12:51           2297 compressedData1
-a---l        02/10/2024     13:15           4403 compressedData2
-a---l        02/10/2024     12:49           8016 data
We can see that the size of compressedData2 has increased after appending more data.

Reading Compressed Files
Reading data from a compressed file in Q/KDB+ is identical to reading from an uncompressed file, both done using the get keyword:

q)get `:/path/to/file
Example

Let’s read the files created in the previous examples:

q)get `:data
7 62 89 15 9 37 23..
q)get `:compressedData1
7 62 89 15 9 37 23..
As shown, from the user’s perspective, reading data is the same whether it’s stored in compressed or uncompressed form.

However, there is a performance difference when reading compressed vs. uncompressed data:

// Create large uncompressed and compressed files
q)bigData:100000000?100
q)`:bigData set bigData
`:bigData
q)(`:bigDataCompressed;16;1;0) set bigData
`:bigDataCompressed

// Time and space statistics
q)\ts get `:bigData
0 560
q)\ts get `:bigDataCompressed
36 459472
In this case, reading the compressed file takes slightly longer and consumes significantly more memory than reading the uncompressed version.

Logical Block Size
The logical block size (LBS) is a crucial parameter that dictates the amount of data compressed in each block. This size plays a significant role in both compression efficiency and performance during data access.

Impact on Compression
A larger LBS allows for better identification of repeated values within the data, which can lead to a better compression ratio. By processing more data at once, the compressor has a greater chance of finding redundancies and achieving more effective compression.

However, a larger block size also influences the decompression process. If the LBS is set too high compared to the amount of data typically accessed during queries, it can lead to excessive and unnecessary decompression work. This inefficiency can slow down query performance as the system processes more data than needed at once.

Possible Values
The LBS can be set to a power of two between 12 and 20. Here’s a table mapping each LBS value to its corresponding block size in bytes:


Attempting to set an LBS outside this range results in an error. For example:

q)(`:data;11;1;0) set data
'bad blockSize 2048 for data$
  [0]  (`:data;11;1;0) set data
                       ^

q)(`:data;21;1;0) set data
'bad blockSize 2097152 for data$
  [0]  (`:data;21;1;0) set data
                       ^
Choosing the appropriate LBS value can impact both compression efficiency and speed. Larger values can improve compression ratios by capturing more repeated data patterns but may also slow down compression.

System Constraints
The minimum LBS is bound by the system’s allocation granularity. If the block size is smaller than this granularity, it could result in wasted space. Typical values for allocation granularity include:

AMD64: 4 kB
SPARC: 8 kB
Windows: 64 kB (default)
Apple Silicon: 16 kB
When selecting a LBS, it’s important to consider the minimum allocation granularity across all platforms that will access the files. Failing to do so may result in sub-optimal performance.

Compression Algorithms
As of Q/KDB+ version 4.1, the following compression algorithms are available:


The Number column indicates the value to be supplied as the algorithm parameter during compression. The Level column denotes the range of possible compression levels for each algorithm.

Example Usage
You can specify the algorithms and levels as follows:

q)(16;1;0)  // Using q IPC compression algorithm with level 0
q)(16;2;6)  // Using gzip compression algorithm with level 6
q)(16;5;-2) // Using zstd compression algorithm with level -2
The q IPC algorithm is built into Q/KDB+ and is always available. In contrast, the other algorithms depend on external libraries, which must be installed on your system to function properly. For more information on meeting the requirements for each algorithm, see here.

If Q/KDB+ cannot access an algorithm, it will raise an error. For example:

q)(`:data;16;2;6) set data // Attempting to use gzip algorithm
'zlib libs required to compress data$. The specified module could not be found.
  [0]  (`:data;16;2;6) set data
                       ^
q)(`:data;16;3;0) set data // Attempting to use snappy algorithm
'snappy libs required to compress data$. The specified module could not be found.
  [0]  (`:data;16;3;0) set data
                       ^
Compression Defaults
In Q/KDB+, we can set default compression parameters using .z.zd, allowing us to compress files without explicitly passing these parameters to set each time. This can simplify file writing operations when compression is needed consistently across multiple files.

Get Jonathan Kane’s stories in your inbox
Join Medium for free to get updates from this writer.

Enter your email
Subscribe
By default, these compression parameters are not set:

q).z.zd
'.z.zd
  [0]  .z.zd
       ^
For example, in earlier cases, we saved a file (data) without compression. Now, let's apply default compression settings:

q).z.zd:(16;1;0) // Set default values (blockSize, algorithm, zipLevel)
q)`:compressedData3 set data
This will compress the data and save it to a new file, compressedData3, without needing to specify compression parameters explicitly to set.

C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     12:51           2297 compressedData1
-a---l        02/10/2024     13:15           4403 compressedData2
-a---l        02/10/2024     13:22           2297 compressedData3
-a---l        02/10/2024     12:49           8016 data
As shown, even though no explicit compression parameters were passed to set, the data was saved in compressed form in compressedData3 using the default settings.

Removing Defaults
You can reset or remove the compression defaults by either setting .z.zd values to zero or expunging the setting altogether:

q).z.zd:3#0 // Reset default compression settings to 0
or

q)\x .z.zd // Expunge .z.zd
After removing the defaults, the system reverts to not applying compression unless explicitly specified.

Handling File Extensions
It’s important to note that files with extensions, such as .txt, will not be compressed by default, even if the compression defaults are set:

q).z.zd
'.z.zd
  [0]  .z.zd
       ^
q)`:data1.txt set data
`:data1.txt
q).z.zd:(16;1;0)
q)`:data2.txt set data
`:data2.txt
C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     13:34           8016 data1.txt
-a---l        02/10/2024     13:34           8016 data2.txt
Both data1.txt and data2.txt are the same size because no compression was applied to data2.txt, even though defaults were set. Files with extensions remain uncompressed unless explicitly handled.

Compression Statistics
Q/KDB+ provides an internal function, -21!, to view compression statistics for files stored on disk. This allows you to inspect how effectively a file has been compressed. The syntax is as follows:

q)-21!`:/path/to/file
For uncompressed files, this command returns nothing:

q)-21!`:data
q)
However, for compressed files, it returns a dictionary with detailed statistics:

q)-21!`:compressedData1
compressedLength  | 2297
uncompressedLength| 8016
algorithm         | 1i
logicalBlockSize  | 16i
zipLevel          | 0i

q)-21!`:compressedData2
compressedLength  | 4403
uncompressedLength| 16016
algorithm         | 1i
logicalBlockSize  | 16i
zipLevel          | 0i
The statistics include:

compressedLength — Size of the file after compression.
uncompressedLength — Original size of the data before compression.
algorithm — The algorithm used for compression.
logicalBlockSize — The size of each logical block used during compression.
zipLevel — The level of compression applied.
We can easily calculate the compression factor (the ratio of uncompressed size to compressed size) from this data:

q)stats:-21!`:compressedData1
q)(%). stats`uncompressedLength`compressedLength
3.489769
In this example, the compression factor is approximately 3.49, meaning the file size has been reduced to about 1/3.49 or 29% of its original size.

Compressing and Decompressing Tables
In Q/KDB+, tables are a common data structure, and the system offers robust methods for compressing and decompressing tables, whether they are flat or splayed. Below, we outline key strategies for efficiently handling table compression.

Flat Tables
Flat tables are stored in a single file, so compression and decompression work the same way as with simpler lists or vectors. To save a flat table in compressed form, you simply pass the desired parameters to set:

q)(targetFile;blockSize;algorithm;level) set table
Example

Let’s create a simple flat table containing symbols, quantities, and prices (sym, qty, and px):

q)n:10000 // Number of rows
q)show t:([] sym:n?`3; qty:n?100; px:n?100f)
sym qty px
----------------
cbl 6   39.27524
afi 67  51.70911
pga 95  51.59796
..
Now we save it both uncompressed and compressed:

q)`:table set t // Uncompressed
`:table
q)(`:tableCompressed;16;1;0) set t // Compressed
`:tableCompressed
We can then compare the file sizes:

C:\> ls

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        02/10/2024     16:54         200046 table
-a---l        02/10/2024     16:54         159152 tableCompressed
As expected, the compressed file is smaller.

Reading Compressed Tables

Reading tables back into memory is seamless with the get function, whether they are compressed or not:

q)get`:table
sym qty px
----------------
cbl 6   39.27524
afi 67  51.70911
pga 95  51.59796
..

q)get`:tableCompressed
sym qty px
----------------
cbl 6   39.27524
afi 67  51.70911
pga 95  51.59796
..
Both commands will return the same table, but with different performance and memory usage characteristics.

Splayed Tables
For large tables, it’s often useful to “splay” them, meaning each column is stored separately on disk. Compression can still be applied, but each column is handled individually.

Creating an Uncompressed Splayed Table

We can splay the table t to a directory:

q)`:uncompressed/t/ set .Q.en[`:uncompressed;t]
`:uncompressed/t/
This will store each column as a separate file inside the t directory.

C:\> ls .\uncompressed\

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
dar--l        03/10/2024     11:59                t
-a---l        03/10/2024     11:59          14924 sym

C:\> ls .\uncompressed\t\

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        03/10/2024     11:59             19 .d
-a---l        03/10/2024     11:59          80016 px
-a---l        03/10/2024     11:59          80016 qty
-a---l        03/10/2024     11:59          84096 sym
Creating a Compressed Splayed Table

To compress a splayed table, we pass the same compression parameters used earlier:

q)(`:compressed/t/;16;1;0) set .Q.en[`:compressed;t]
`:compressed/t/
After saving the compressed splayed table, we can again compare file sizes:

C:\> ls .\compressed\

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
dar--l        03/10/2024     12:02                t
-a---l        03/10/2024     12:02          14924 sym

C:\> ls .\compressed\t\

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        03/10/2024     12:02             75 .d
-a---l        03/10/2024     12:02          80080 px
-a---l        03/10/2024     12:02          21559 qty
-a---l        03/10/2024     12:02          33397 sym
Column Files

The compression significantly reduces the size of the qty and sym column files, but interestingly, the px file increases in size after compression. (The .d file, which holds the column order, also grows slightly but remains small in comparison.) This highlights a key point: not all columns compress well, and it would be ideal to apply compression selectively based on the characteristics of each column.

Column-by-Column Compression

Since each column in a splayed table is stored as a separate file, Q/KDB+ allows for column-by-column compression. Different compression algorithms, levels, and block sizes can be assigned to each column individually. This flexibility is useful because certain columns, depending on factors such as data type, value repetition, and overall structure, may respond better to specific compression strategies. By selectively applying compression, you can optimise performance and storage efficiency without being limited to a single compression method across the entire table.

To apply column-specific compression to a splayed table, use the following syntax:

q)(dir;dic) set t
Where

dir — The path to the splayed table directory.
dic — A dictionary that maps each column to its respective compression parameters.
t — The table to be splayed.
In the earlier example, the px column didn’t compress effectively, so we’ll exclude it from compression:

q)show dic:`px`qty`sym!(0 0 0;16 1 0;16 1 0)
px | 0  0 0   // No compression on px
qty| 16 1 0
sym| 16 1 0
q)(`:compressedCByC/t/;dic) set .Q.en[`:compressedCByC;t]
`:compressedCByC/t/
This results in:

C:\> ls .\compressedCByC\

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
dar--l        03/10/2024     12:46                t
-a---l        03/10/2024     12:46          14924 sym

C:\> ls .\compressedCByC\t\

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        03/10/2024     12:46             19 .d
-a---l        03/10/2024     12:46          80016 px
-a---l        03/10/2024     12:46          21559 qty
-a---l        03/10/2024     12:46          33397 sym
The px column file remains the same size as in the uncompressed table. We can confirm that no compression was applied to px, while the qty and sym columns were successfully compressed using q IPC compression:

q)-21!`:compressedCByC/t/px

q)-21!`:compressedCByC/t/qty
compressedLength  | 21559
uncompressedLength| 80016
algorithm         | 1i
logicalBlockSize  | 16i
zipLevel          | 0i

q)-21!`:compressedCByC/t/sym
compressedLength  | 33397
uncompressedLength| 84096
algorithm         | 1i
logicalBlockSize  | 16i
zipLevel          | 0i
dic can also include a null key to define default compression parameters for any columns not explicitly listed in the dictionary. This allows for flexible control over compression behaviour across multiple columns. For example:

q)show dic:``qty`sym!(0 0 0;16 1 0;16 1 0)
   | 0  0 0   // Default: No compression
qty| 16 1 0
sym| 16 1 0
In this case, columns not specified in the dictionary, like px, would automatically use the default compression (0 0 0), which applies no compression. This offers a concise way to manage compression policies while avoiding the need to specify every column individually.

Compress a Splayed Table on Disk

If you already have a splayed table saved on disk that is too large to fit into memory, you can pass the splayed table’s path directly to set, allowing compression without needing to load the data in memory. Here's an example of applying compression to the uncompressed splayed directory we created earlier:

q)(`:uncompressed/t/;dic) set `:uncompressed/t
`:uncompressed/t/
Now let’s check the files:

C:\> ls .\uncompressed\t

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a----        03/10/2024     13:27             19 .d
-a----        03/10/2024     13:27          80016 px
-a---l        03/10/2024     13:27          21559 qty
-a---l        03/10/2024     13:27          33397 sym
You can see that the qty and sym files have been successfully compressed on disk.

The Sym File

The file size of the sym file remains unchanged between the compressed and uncompressed versions. Let’s inspect the compression statistics to confirm:

q)-21!`:compressed/sym
q)
Since no output is returned, it indicates that the sym file was not compressed.

It’s important to note that the sym file should not be compressed for two key reasons:

Unique values: The sym file contains only distinct values (symbols), which typically doesn’t compress well since there’s no repetition in the data.
Appending restrictions: Once compressed, the sym file becomes non-appendable, as illustrated below.
q)(`:bad/t/;16;1;0) set .Q.en[`:bad;t] // Create a new splay
`:bad/t/
q)(`:bad/sym;16;1;0) set `:bad/sym     // Manually compress the sym file
`:bad/sym
q)-21!`:bad/sym                        // Check the sym file compression stats
compressedLength  | 14980
uncompressedLength| 14924
algorithm         | 1i
logicalBlockSize  | 16i
zipLevel          | 0i
Though the file is compressed, compressedLength is actually greater than uncompressedLength, confirming poor compression efficiency.

Furthermore, trying to append data to a compressed sym file results in an error:

q)(`:bad/t2/;16;1;0) set .Q.en[`:bad;([] sym:`a`b`c; val:1 2 3)] 
'no append to zipped enums: bad/sym
  [0]  (`:bad/t2/;16;1;0) set .Q.en[`:bad;([] sym:`a`b`c; val:1 2 3)]
                          ^
This shows that once a sym file is compressed, Q/KDB+ raises an error when attempting to append new symbol data. Compression offers little benefit for sym files and can restrict common operations, making it unsuitable for this specific file.

Appending to a Compressed Splayed Table

Appending new data to compressed splayed tables works exactly the same as for uncompressed ones. Here’s an example of appending a few rows to the compressedCByC splayed table:

q)`:compressedCByC/t upsert .Q.en[`:.;] ([] sym:`a`b`c; qty:1 2 3; px:1 2 3f)
`:compressedCByC/t
q)\l compressedCByC // Load the splayed database
q)-5#t // Inspect the last 5 rows
sym qty px
----------------
ngh 89  99.18507
jic 28  23.3976
a   1   1
b   2   2
c   3   3
Checking the directory confirms that the file sizes have slightly increased due to the addition of the new rows:

C:\> ls .\compressedCByC\t

Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a---l        04/10/2024     16:00             19 .d
-a---l        04/10/2024     16:00          80040 px
-a---l        04/10/2024     16:00          21565 qty
-a---l        04/10/2024     16:00          33407 sym
As expected, the qty, sym, and px files have grown, reflecting the addition of new data.

Conclusion
In this blog, we explored the fundamentals of compression in Q/KDB+, focusing on how to write and read compressed files effectively. We discussed the significance of compression in optimising storage and enhancing data management, particularly regarding tables and columns. We also examined the implications of logical block size, emphasizing the trade-offs between compression efficiency and performance during data access.

Understanding these concepts is vital for effectively managing data in Q/KDB+. By applying the right compression techniques, you can maximise storage efficiency while minimising the performance overhead during query operations.

Check out part two of this series on compression in Q/KDB+, where we will explore algorithm performance, analyse the efficiency of querying compressed data, benchmark various compression methods, and discuss practical strategies to optimise your compression choices.