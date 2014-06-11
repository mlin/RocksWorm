/*
GivenManifestHTTPEnv: an HTTP env implementation based on a given manifest of
files prefixed by a certain base URL, where the manifest has URL suffixes and
sizes of available files
*/

#ifndef GIVENMANIFESTHTTPENV_H
#define GIVENMANIFESTHTTPENV_H

#include "rocksdb-on-cloud/BaseHTTPEnv.h"
#include <map>

class GivenManifestHTTPEnv : public BaseHTTPEnv {
public:
    using manifest = std::map<std::string, std::uint64_t>;

protected:
    manifest manifest_;

public:
    GivenManifestHTTPEnv(const std::string& base_url, const manifest& the_manifest, const HTTPEnvOptions& opts)
        : BaseHTTPEnv(base_url,opts)
        , manifest_(the_manifest) {}

    virtual ~GivenManifestHTTPEnv() {}

    rocksdb::Status GetChildren(const std::string& dir, std::vector<std::string>* result) override {
        size_t dirsz = dir.size();
        if (dirsz == 0 || result == nullptr) {
            return rocksdb::Status::InvalidArgument("GivenManifestHTTPEnv::GetChildren");
        }

        // ensure the dirname ends in slash, for correct key filtering
        std::string dirslash(dir);
        if (dirslash[dirsz-1] != '/') {
            dirslash += '/';
            dirsz++;
        }

        // set result to those keys prefixed by dirslash
        result->clear();
        for (auto it = manifest_.begin(); it != manifest_.end(); it++) {
            const std::string& key = it->first;
            // TODO filter out subdirectories
            if (key.size() > dirsz && key.substr(0, dirsz) == dirslash) {
                result->push_back(key);
            }
        }
        if (result->size() == 0) return rocksdb::Status::NotFound(dir);
        return rocksdb::Status::OK();
    }

    rocksdb::Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
        if (file_size == nullptr) return rocksdb::Status::InvalidArgument("RocHTTPEnv::GetFileSize");
        auto it = manifest_.find(fname);
        if (it == manifest_.end()) return rocksdb::Status::NotFound(fname);
        *file_size = it->second;
        return rocksdb::Status::OK();
    }
};

#endif

