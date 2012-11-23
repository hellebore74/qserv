
[frontend]
# xrootd=lsst-dev01:1094
# xrootd_user=qsmaster
# scratch_path=/dev/shm/qserv
# port=7080
xrootd = <XROOTD_MANAGER_HOST>:<XROOTD_PORT>
scratch_path = /dev/shm/qserv
xrootd_user = qsmaster 

# [mgmtdb]
# db=qservMeta
# # Steal resultdb settings for now.

[resultdb]
# host=
# port=0
# unix_socket=/u1/local/mysql.sock
# db=qservResult
# user=qsmaster
# passwd=
# dropMem=
passwd = 
db = qservResult
unix_socket = <INSTALL_DIR>/var/lib/mysql/mysql.sock
host = 
user = qsmaster
port = 0

[partitioner]
# stripes=18
# substripes=10
# emptyChunkListFile=
substripes = 60
stripes = 18
emptyChunkListFile=  <INSTALL_DIR>/etc/emptyChunks.txt

[table]
# chunked=Source,ForcedSource
# subchunked=Object
# alloweddbs=LSST
chunked=Source,FaintSource
subchunked=Object
alloweddbs=LSST,Test1
partitionCols=Object:ra_PS,decl_PS,objectId;Source:raObject,declObject,objectId

# [tuning]
# memoryEngine=yes
 
# [debug]
# chunkLimit=-1
 
[mysql]
# mysqlclient=
mysqlclient=
