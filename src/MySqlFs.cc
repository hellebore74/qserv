#include "lsst/qserv/worker/MySqlFs.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"

#include "lsst/qserv/worker/MySqlFsDirectory.h"
#include "lsst/qserv/worker/MySqlFsFile.h"

#include <cerrno>

// Externally declare XrdSfs loader to cheat on Andy's suggestion.
extern XrdSfsFileSystem *XrdXrootdloadFileSystem(XrdSysError *, char *, 
						 const char *);


namespace qWorker = lsst::qserv::worker;

qWorker::MySqlFs::MySqlFs(XrdSysError* lp, char const* cFileName) 
  : XrdSfsFileSystem(), _eDest(lp) {

    _eDest->Say("MySqlFs loading libXrdOfs.so for clustering cmsd support.");
#ifdef NO_XROOTD_FS
#else
    XrdSfsFileSystem* fs;
    fs = XrdXrootdloadFileSystem(_eDest, "libXrdOfs.so", cFileName);
    if(fs == 0) {
	_eDest->Say("Problem loading libXrdOfs.so. Clustering won't work.");
    }
#endif

}

qWorker::MySqlFs::~MySqlFs(void) {
}

// Object Allocation Functions
//
XrdSfsDirectory* qWorker::MySqlFs::newDir(char* user) {
    return new qWorker::MySqlFsDirectory(_eDest, user);
}

XrdSfsFile* qWorker::MySqlFs::newFile(char* user) {
    return new qWorker::MySqlFsFile(_eDest, user);
}

// Other Functions
//
int qWorker::MySqlFs::chmod(
    char const* Name, XrdSfsMode Mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::exists(
    char const* fileName, XrdSfsFileExistence& existsFlag,
    XrdOucErrInfo& outError, XrdSecEntity const* client,
    char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::fsctl(
    int const cmd, char const* args, XrdOucErrInfo& outError,
    XrdSecEntity const* client) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::getStats(char* buff, int blen) {
    return SFS_ERROR;
}

char const* qWorker::MySqlFs::getVersion(void) {
    return "$Id$";
}

int qWorker::MySqlFs::mkdir(
    char const* dirName, XrdSfsMode Mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& outError,
                         XrdSecEntity const* client) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::rem(char const* path, XrdOucErrInfo& outError,
                     XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::remdir(char const* dirName, XrdOucErrInfo& outError,
                        XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::rename(
    char const* oldFileName, char const* newFileName, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaqueO, char const* opaqueN) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::stat(
    char const* Name, struct stat* buf, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::stat(char const* Name, mode_t& mode, XrdOucErrInfo& outError,
                      XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::truncate(
    char const* Name, XrdSfsFileOffset fileOffset, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

class XrdSysLogger;

extern "C" {

XrdSfsFileSystem* XrdSfsGetFileSystem(
    XrdSfsFileSystem* native_fs, XrdSysLogger* lp, char const* fileName) {
    static XrdSysError eRoute(lp, "MySqlFs");
    static qWorker::MySqlFs myFS(&eRoute, fileName);

    eRoute.Say("MySqlFs (MySQL File System)");
    eRoute.Say(myFS.getVersion());
    return &myFS;
}

} // extern "C"

