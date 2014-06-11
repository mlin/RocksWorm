/*
RocHTTPEnv: an HTTP env that reads from a single .roc file, which is a simple
file format concatenating the constituent files of a RocksDB database. See
roc.cc
*/

#ifndef ROCHTTPENV_H
#define ROCHTTPENV_H

#include "rocksdb/env.h"

#include "rocksdb-on-cloud/BaseHTTPEnv.h"
#include <sstream>
#include <map>

// file name -> <starting offset within roc, file size>
using roc_manifest = std::map<std::string, std::pair<std::uint64_t, std::uint64_t>>;

class RocHTTPEnv : public BaseHTTPEnv {
protected:
    roc_manifest manifest_;

    rocksdb::Status GetRocTail(size_t n, rocksdb::Slice* ans, char* scratch);
    rocksdb::Status EnsureManifest();

public:
    // roc_url should be the complete URL to the .roc file. The Db using this
    // Env should then be opened with empty string as the dbpath
    RocHTTPEnv(const std::string& roc_url, const HTTPEnvOptions& opts)
        : BaseHTTPEnv(roc_url,opts) {}
    virtual ~RocHTTPEnv() {}

    rocksdb::Status GetChildren(const std::string& dir, std::vector<std::string>* result) override {
        assert(result);
        if (dir.find('/') != dir.rfind('/')) return rocksdb::Status::InvalidArgument("RocHTTPEnv::GetChildren");

        rocksdb::Status s = EnsureManifest();
        if (!s.ok()) return s;

        result->clear();
        for (auto it = manifest_.begin(); it != manifest_.end(); it++) {
            result->push_back(it->first);
        }
        assert(result->size());
        return rocksdb::Status::OK();
    }

    rocksdb::Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
        assert(file_size);
        if (fname.find('/') == std::string::npos) return rocksdb::Status::InvalidArgument("RocHTTPEnv::GetFileSize");
        rocksdb::Status s = EnsureManifest();
        if (!s.ok()) return s;

        auto it = manifest_.find(fname.substr(fname.find('/')+1));
        if (it == manifest_.end()) return rocksdb::Status::NotFound(fname);
        *file_size = it->second.second;
        return rocksdb::Status::OK();
    }

    rocksdb::Status PrepareHead(const std::string& fname,
                                std::string& url, HTTP::headers& request_headers) override {
        // only used in EnsureManifest/GetRocTail to get the total size of the roc file
        assert(fname.size() == 0);
        url = base_url_;
        request_headers.clear();
        return rocksdb::Status::OK();
    }

    rocksdb::Status PrepareGet(const std::string& fname, uint64_t offset, size_t n,
                               std::string& url, HTTP::headers& request_headers) override {
        if (fname.size() == 0) {
            // only used in EnsureManifest/GetRocTail to read the roc manifest
            return BaseHTTPEnv::PrepareGet(fname, offset, n, url, request_headers);
        }
        if (fname.find('/') == std::string::npos) return rocksdb::Status::InvalidArgument("RocHTTPEnv::PrepareGet");

        rocksdb::Status s = EnsureManifest();
        if (!s.ok()) return s;
        request_headers.clear();
        s = BaseHTTPEnv::PrepareGet("", offset, n, url, request_headers);
        if (!s.ok()) return s;

        auto it = manifest_.find(fname.substr(fname.find('/')+1));
        if (it == manifest_.end()) return rocksdb::Status::NotFound(fname);
        uint64_t roc_offset = it->second.first, file_size = it->second.second;

        if (offset+n > file_size) {
            return rocksdb::Status::InvalidArgument("RocHTTPEnv::PrepareGet");
        }

        url = base_url_;

        std::ostringstream fmt_range;
        fmt_range << "bytes=" << roc_offset+offset << "-" << (roc_offset+offset+n-1);
        request_headers["range"] = fmt_range.str();

        return rocksdb::Status::OK();
    }
};

#endif

