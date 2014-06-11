#include "rocksdb-on-cloud/RocHTTPEnv.h"
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
using namespace std;
using namespace rocksdb;

// get the last n bytes of the roc file
Status RocHTTPEnv::GetRocTail(size_t n, Slice* ans, char* scratch) {
    assert(n);
    assert(scratch);
    assert(ans);
    // HEAD the file to determine its size
    HTTP::headers headers;
    Status s = RetryHead("", headers);
    if (!s.ok()) return s;
    auto it = headers.find("content-length");
    if (it == headers.end()) return Status::IOError("HTTP HEAD response didn't include Content-Length header");
    unsigned long long rocsz = strtoull(it->second.c_str(), nullptr, 10);

    if (!rocsz) return Status::Corruption("HTTP server reports empty roc file");

    // GET the file tail
    return RetryGet("", (rocsz>=n ? rocsz-n : 0), std::min((unsigned long long)n,rocsz), headers, ans, scratch);
}

// Read the .roc manifest if we haven't already. See comments in roc.cc for
// details about the format
Status RocHTTPEnv::EnsureManifest() {
    if (manifest_.size()) return Status::OK();

    size_t rdsz = 16384;
    unique_ptr<char[]> scratch(new char[rdsz]);
    Slice roc_tail;

    // fetch tail of roc file
    Status s = GetRocTail(rdsz, &roc_tail, scratch.get());
    if (!s.ok()) return s;

    // read magic and manifest size
    if (roc_tail.size() < 12) return Status::Corruption("invalid roc file");
    const char *magic = roc_tail.data() + roc_tail.size() - 4;
    if (magic[0] != 'R' || magic[1] != 'O' || magic[2] != 'C' || magic[3] != '0') {
        return Status::Corruption("not a roc file");
    }
    uint64_t manifest_size = *(uint64_t*)(roc_tail.data() + roc_tail.size() - 12);

    // ensure we have the entire manifest
    while (roc_tail.size() < manifest_size+12) {
        uint64_t last = roc_tail.size();
        rdsz *= 4;
        scratch.reset(new char[rdsz]);
        s = GetRocTail(rdsz, &roc_tail, scratch.get());
        if (!s.ok()) return s;
        if (roc_tail.size() <= last) return Status::Corruption("invalid roc file");
    }

    // read each file entry
    char *pos = (char*)(roc_tail.data()+roc_tail.size()-12-manifest_size);
    const char *last_pos = pos+manifest_size;
    uint64_t current_offset = 0;
    roc_manifest ans;
    while (pos < last_pos) {
        if (pos+8 >= last_pos) return Status::Corruption("invalid roc file");
        uint64_t filesz = *(uint64_t*)pos;
        pos += 8;

        if (pos+8 >= last_pos) return Status::Corruption("invalid roc file");
        uint64_t namelen = *(uint64_t*)pos;
        pos += 8;
        if (pos+namelen > last_pos) return Status::Corruption("invalid roc file");
        std::string name;
        name.assign(pos,namelen);
        pos += namelen;
        
        if (ans.find(name) != ans.end()) return Status::Corruption("duplicate manifest entries in roc file");
        ans[name] = pair<uint64_t,uint64_t>(current_offset,filesz);
        current_offset += filesz;       
    }

    if (ans.size() == 0) return Status::Corruption("empty roc file");

    if (opts_.http_stderr_log_level <= InfoLogLevel::INFO) {
        ostringstream msg;
        msg << CensorURL(base_url_) << " roc manifest:" << endl;
        for (auto entry : ans) {
            msg << entry.first << ' ' << entry.second.first << ' ' << entry.second.second << endl;
        }
        Info(&http_logger_, "%s", msg.str().c_str());
    }

    manifest_ = ans;
    return Status::OK();
}