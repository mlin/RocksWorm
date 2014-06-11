// roc
//
// Given the path to a RocksDB database at rest, generate a .roc file for
// upload to cloud storage. "At rest" means no process is writing to the
// database, and the last writer process called Flush before exiting.
//
// The .roc file simply consists of the concatenated contents of several files,
// followed by a trailing manifest. The format of the manifest is as follows:
//
// MANIFEST   ::= FILE_LIST uint64 MAGIC       the integer is the total size of 
//                                             file_list, in bytes
// FILE_LIST  ::= FILE_ENTRY FILE_LIST | ""
// FILE_ENTRY ::= uint64 STRING                byte length and name of the file
// STRING     ::= uint64 (byte*)               byte length and UTF-8 characters
// MAGIC      ::= "\x52" "\x4F" "\x43" "0x30"  the four characters "ROC0"
//
// uint64 is an 8-byte, little-endian, unsigned integer.
//
// The entries in the file list are in the same order as the preceding file
// contents. Reading the manifest at the end of the file provides the
// information needed to access the contents by filename and offset.

#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
using namespace std;

#include "rocksdb/db.h"
#include "rocksdb/env.h"
using namespace rocksdb;

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// http://esr.ibiblio.org/?p=5095
#define IS_BIG_ENDIAN (*(uint16_t *)"\0\xff" < 0x100)

void usage() {
    cout << "Usage: roc /rocksdb/database/path [dest.roc]" << endl;
    cout << "Emits roc file to standard out if destination path isn't specified." << endl;
}

struct file_entry {
    string name;
    uint64_t size;
    file_entry(const string& name_, uint64_t size_) : name(name_), size(size_) {}
};

int file_size(const string& dn, const string& fn, size_t& ans) {
    struct stat st;
    string fp = dn + "/" + fn;
    int c = stat(fp.c_str(),&st);
    if (c != 0) return c;
    ans = st.st_size;
    return 0;
}

bool findMANIFEST(const string& dbpath, string& ans) {
    ifstream current;
    current.open(dbpath + "/" + "CURRENT");
    if (!current.is_open()) return false;
    getline(current,ans);
    return !current.fail() && !current.bad() && ans.size() > 0;
}

int emit(const string& dbpath, const vector<file_entry>& manifest, ostream& dest) {
    // emit the file contents
    const size_t bufsize = 1048576;
    unique_ptr<char[]> buf(new char[bufsize]);
    for (auto it : manifest) {
        uint64_t ct = 0;
        ifstream src;
        src.open(dbpath + "/" + it.name);
        if (!src.is_open()) {
            cerr << "Error: couldn't open " << it.name << " for writing" << endl;
            return 1;
        }
        while (src.good()) {
            src.read(buf.get(),bufsize);
            if (src.bad() || (src.fail() && !src.eof())) {
                cerr << "Error while reading " << it.name << endl;
                return 1;
            }
            dest.write(buf.get(),src.gcount());
            if (!dest.good()) {
                cerr << "Error while writing " << it.name << " to destination" << endl;
                return 1;
            }
            ct += src.gcount();
        }
        if (ct != it.size) {
            cerr << "Error: read " << ct << " instead of the expected " << it.size << " bytes from" << it.name << endl;
            return 1;
        }
    }

    // emit the manifest
    uint64_t manifest_sz = 0;
    for (auto it : manifest) {
        uint64_t sz = it.size;
        dest.write(reinterpret_cast<char*>(&sz),8);
        manifest_sz += 8;

        sz = it.name.size();
        dest.write(reinterpret_cast<char*>(&sz),8);
        dest.write(it.name.c_str(),sz);
        manifest_sz += sz + 8;
    }
    dest.write(reinterpret_cast<char*>(&manifest_sz),8);
    dest << "ROC0";
    dest.flush();
    if (!dest.good()) {
        cerr << "Error writing trailing manifest to destination" << endl;
        return 1;
    }

    return 0;
}

int main(int argc, char** argv) {
    if (IS_BIG_ENDIAN) {
        // FIXME
        cerr << "Error: this program is broken on big-endian architectures" << endl;
        return 1;
    }

    Status s;
    DB *rawdb = nullptr;
    Options dbopts;
    ReadOptions rdopts;

    if (argc < 2 || *(argv[1]) == 0) {
        usage();
        return 1;
    }
    string dbpath(argv[1]);
    if (dbpath[dbpath.size()-1] == '/') {
        dbpath.erase(dbpath.size()-1);
    }

    // open database
    s = rocksdb::DB::OpenForReadOnly(dbopts,dbpath,&rawdb);
    if (!s.ok()) {
        cerr << "Error opening database: " << s.ToString() << endl;
        return 1;
    }
    unique_ptr<DB> db(rawdb);

    // ensure there are no live WAL files
    VectorLogPtr live_wal_files;
    s = db->GetSortedWalFiles(live_wal_files);
    if (!s.ok()) {
        cerr << "Error in GetSortedWalFiles: " << s.ToString() << endl;
        return 1;
    }
    if (live_wal_files.size()) {
        cerr << "Error: database is either in use or needs recovery (found live WAL files)" << endl;
        return 1;
    }

    // make a list of the files to concatenate
    vector<file_entry> manifest;
    vector<LiveFileMetaData> md;
    db->GetLiveFilesMetaData(&md);
    for (auto it : md) {
        string fn(it.name);
        if (fn.size() > 0 && fn[0] == '/') {
            fn.erase(0,1);
        }
        // TODO: ensure no additional slashes in fn
        manifest.push_back(file_entry(fn,it.size));
    }
    vector<string> stdfiles = {"IDENTITY", "CURRENT"};
    size_t sz;
    for (auto it : stdfiles) {
        if (file_size(dbpath,it,sz)) {
            cerr << "Error: couldn't determine file size of " << it << endl;
            return 1;
        }
        manifest.push_back(file_entry(it,sz));
    }
    string fn_manifest;
    if (!findMANIFEST(dbpath,fn_manifest)) {
        cerr << "Error: couldn't determine database MANIFEST filename" << endl;
        return 1;
    }
    if (file_size(dbpath,fn_manifest,sz)) {
        cerr << "Error: couldn't determine size of manifest file " << fn_manifest << endl;
        return 1;
    }
    manifest.push_back(file_entry(fn_manifest,sz));
    
    // emit roc file to either destination file or standard out
    if (argc >= 3) {
        ofstream dest;
        dest.open(argv[2]);
        if (!dest.is_open()) {
            cerr << "Error: couldn't open " << argv[2] << " for writing" << endl;
            return 1;
        }
        return emit(dbpath,manifest,dest);
    } else {
        return emit(dbpath,manifest,cout);
    }
}
