#include "RocksWorm/BaseHTTPEnv.h"
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <sstream>
using namespace std;
using namespace rocksdb;

class BaseHTTPRandomAccessFile : public RandomAccessFile {
    BaseHTTPEnv *env_;
    string fname_;
    uint64_t sz_;

    // TODO: detect sequential access patterns (which may occur through
    // RandomAccessFile being used by db iterators) and perform some read-
    // ahead/caching. Potentially factorable into a subclass of generic
    // utility for RocksDB.
public:
    BaseHTTPRandomAccessFile(BaseHTTPEnv* env, const string& fname, uint64_t sz) 
        : env_(env)
        , fname_(fname)
        , sz_(sz)
    {
    }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
        assert(result);
        assert(scratch);
        n = min(n, sz_-offset);
        if (sz_ < offset || n == 0) {
            *result = Slice();
            return Status::OK();
        }

        HTTP::headers response_headers;
        return env_->RetryGet(fname_, offset, n, response_headers, result, scratch);
    }
};

class BaseHTTPSequentialFile : public SequentialFile {
    BaseHTTPRandomAccessFile f_;
    uint64_t pos_;

public:
    BaseHTTPSequentialFile(BaseHTTPEnv* env, const string& fname, uint64_t sz) 
        : f_(env, fname, sz)
        , pos_(0)
        {}

    Status Read(size_t n, Slice *result, char* scratch) {
        Status s = f_.Read(pos_, n, result, scratch);
        if (s.ok()) pos_ += n;
        return s;
    }

    Status Skip(uint64_t n) {
        pos_ += n;
        return Status::OK();
    }
};

BaseHTTPEnv::BaseHTTPEnv(const std::string& base_url, const HTTPEnvOptions& opts)
    : base_url_(base_url)
    , connpool_(opts.connpool)
    , opts_(opts)
    , http_logger_("HTTP", opts_.http_stderr_log_level)
{
    inner_env_ = Env::Default();
    size_t sz = base_url_.size();
    assert(sz > 0);
    if (base_url_[sz-1] == '/') {
        base_url_.erase(sz-1);
    }
    if (connpool_ == nullptr) {
        connpool_ = new HTTP::CURLpool(64);
    }
}

BaseHTTPEnv::~BaseHTTPEnv() {
    if (connpool_ && opts_.connpool == nullptr) {
        delete connpool_;
    }
}

Status BaseHTTPEnv::PrepareHead(const std::string& fname,
                                std::string& url, HTTP::headers& request_headers) {
    if (fname.size()) {
        ostringstream fmt_url;
        fmt_url << base_url_ << "/" << fname;
        url = fmt_url.str();
    } else {
        url = base_url_;
    }
    request_headers.clear();
    return Status::OK();
}

Status BaseHTTPEnv::PrepareGet(const std::string& fname, uint64_t offset, size_t n,
                               std::string& url, HTTP::headers& request_headers) {
    if (n<=0) {
        return Status::InvalidArgument("BaseHTTPEnv::PrepareGet: zero-length read");
    }

    if (fname.size()) {
        ostringstream fmt_url;
        fmt_url << base_url_ << "/" << fname;
        url = fmt_url.str();
    } else {
        url = base_url_;
    }

    request_headers.clear();
    ostringstream fmt_range;
    fmt_range << "bytes=" << offset << "-" << (offset+n-1);
    request_headers["range"] = fmt_range.str();

    return Status::OK();
}

void BaseHTTPEnv::LogHeaders(const HTTP::headers& hdrs) {
    if (opts_.http_stderr_log_level <= InfoLogLevel::DEBUG_LEVEL) {
        ostringstream stm;
        for (auto hdr : hdrs) {
            stm << hdr.first << ": " << hdr.second << endl;
        }
        Debug(&http_logger_, "%s", stm.str().c_str());
    }
}

class RequestTimer {
    uint64_t t0_;

public:
    RequestTimer() {
        timeval t0;
        gettimeofday(&t0, nullptr);
        t0_ = uint64_t(t0.tv_sec)*1000 + uint64_t(t0.tv_usec)/1000;
    }

    unsigned int millis() {
        timeval tv;
        gettimeofday(&tv, nullptr);
        uint64_t t = uint64_t(tv.tv_sec)*1000 + uint64_t(tv.tv_usec)/1000;
        return (unsigned int)(t-t0_);
    }
};

Status BaseHTTPEnv::RetryHead(const string& fname, HTTP::headers& response_headers) {
    Status s;
    string url;
    useconds_t delay = opts_.retry_initial_delay;

    for (unsigned int i = 0; i <= opts_.retry_times; i++) {
        if (i) {
            usleep(delay);
            delay *= opts_.retry_backoff_factor;
        }

        string url;
        HTTP::headers request_headers;
        s = PrepareHead(fname, url, request_headers);
        if (!s.ok()) return s;
        Info(&http_logger_, "HEAD %s", CensorURL(url).c_str());
        LogHeaders(request_headers);
        RequestTimer t;

        long response_code = -1;
        CURLcode c = HTTP::HEAD(url, request_headers, response_code, response_headers, connpool_);
        if (c != CURLE_OK) {
            s = CURLcodeToStatus(c);
        } else if (response_code >= 500 && response_code <= 599) {
            s = HTTPcodeToStatus(response_code);
        } else if (response_code < 200 || response_code >= 300) {
            Error(&http_logger_, "HEAD %s => %d (%dms)", CensorURL(url).c_str(), response_code, t.millis());
            return HTTPcodeToStatus(response_code);
        } else {
            Info(&http_logger_, "HEAD %s => %d (%dms)", CensorURL(url).c_str(), response_code, t.millis());
            LogHeaders(response_headers);
            return Status::OK();
        }

        Warn(&http_logger_, "HEAD %s failed (%dms, try %d of %d)...%s", CensorURL(url).c_str(), t.millis(), i+1, opts_.retry_times+1, s.ToString().c_str());
    }

    Error(&http_logger_, "HEAD %s failed...%s", CensorURL(url).c_str(), s.ToString().c_str());
    return s;
}

Status BaseHTTPEnv::RetryGet(const string& fname, uint64_t offset, size_t n,
                             HTTP::headers& response_headers, Slice* response_body, char* scratch) {
    assert(response_body);
    assert(scratch);
    Status s;
    string url;
    useconds_t delay = opts_.retry_initial_delay;

    for (unsigned int i = 0; i <= opts_.retry_times; i++) {
        if (i) {
            usleep(delay);
            delay *= opts_.retry_backoff_factor;
        }

        HTTP::headers request_headers;
        s = PrepareGet(fname, offset, n, url, request_headers);
        if (!s.ok()) return s;
        Info(&http_logger_, "GET %s [%d-%d]", CensorURL(url).c_str(), offset, offset+n);
        LogHeaders(request_headers);
        RequestTimer t;

        long response_code = -1;
        stringstream response_body_stream;
        response_headers.clear();
        CURLcode c = HTTP::GET(url, request_headers,
                               response_code, response_headers, response_body_stream,
                               connpool_);
        if (c != CURLE_OK) {
            s = CURLcodeToStatus(c);
        } else if (response_code >= 500 && response_code <= 599) {
            s = HTTPcodeToStatus(response_code);
        } else if (response_code < 200 || response_code >= 300) {
            Error(&http_logger_, "GET %s [%d-%d] => %d (%dms)",  CensorURL(url).c_str(), offset, offset+n, response_code, t.millis());
            return HTTPcodeToStatus(response_code);
        } else {
            response_body_stream.read(scratch, n);
            if (response_body_stream.good() || response_body_stream.eof()) {
                size_t c = response_body_stream.gcount();
                auto it = response_headers.find("content-length");
                if (it == response_headers.end()
                      || strtoull(it->second.c_str(), nullptr, 10) == c) {
                    *response_body = Slice(scratch, c);
                    Info(&http_logger_, "GET %s [%d-%d] => %d (%dms, %zu bytes)",  CensorURL(url).c_str(), offset, offset+n, response_code, t.millis(), c);
                    LogHeaders(response_headers);
                    return Status::OK();
                }
                Debug(&http_logger_, "GET %s [%d-%d] => %d (%dms) with unexpected HTTP response body length %zu, response headers content-length %s", CensorURL(url).c_str(), offset, offset+n, response_code, t.millis(), c, it != response_headers.end() ? it->second.c_str() : "(none)");
            }
            s = Status::IOError("Unexpected HTTP response body length");
        }

        Warn(&http_logger_, "GET %s [%d-%d] failed (%dms, try %d of %d)...%s", CensorURL(url).c_str(), offset, offset+n, t.millis(), i+1, opts_.retry_times+1, s.ToString().c_str());
    }

    Error(&http_logger_, "GET %s [%d-%d] failed...%s", CensorURL(url).c_str(), offset, offset+n, s.ToString().c_str());
    return s;
}

Status BaseHTTPEnv::FileExists(const std::string& fname) {
    uint64_t ignore;
    Status s = GetFileSize(fname, &ignore);
    return s.ok() ? Status::OK() : Status::NotFound();
}

Status BaseHTTPEnv::GetFileSize(const std::string& fname, uint64_t *file_size) {
    assert(file_size);
    HTTP::headers response_headers;
    Status s = RetryHead(fname, response_headers);
    if (!s.ok()) return s;

    auto content_length_it = response_headers.find("content-length");
    if (content_length_it == response_headers.end()) {
        return Status::IOError("HTTP HEAD response didn't include content-length header");
    }

    const char *content_length_cstr = content_length_it->second.c_str();
    char *endptr= nullptr;
    errno = 0;
    unsigned long long content_length = strtoull(content_length_cstr, &endptr, 10);
    if (errno || endptr - content_length_cstr != long(content_length_it->second.size()) || content_length < 0) {
        return Status::IOError("HTTP HEAD response had unreadable content-length header");
    }

    *file_size = (uint64_t) content_length;
    return Status::OK();
}

Status BaseHTTPEnv::NewSequentialFile(const std::string& fname, unique_ptr<SequentialFile>* result,
                                      const EnvOptions& options) {
    uint64_t sz;
    Status s = GetFileSize(fname, &sz); // TODO cache this info
    if (!s.ok()) return s;
    result->reset(new BaseHTTPSequentialFile(this, fname, sz));
    return Status::OK();
}

Status BaseHTTPEnv::NewRandomAccessFile(const std::string& fname, unique_ptr<RandomAccessFile>* result,
                                        const EnvOptions& options) {
    uint64_t sz;
    Status s = GetFileSize(fname, &sz); // TODO cache this info
    if (!s.ok()) return s;
    result->reset(new BaseHTTPRandomAccessFile(this, fname, sz));
    return Status::OK();
}

Status BaseHTTPEnv::CURLcodeToStatus(CURLcode c) {
    return Status::IOError(curl_easy_strerror(c));
}

Status BaseHTTPEnv::HTTPcodeToStatus(long response_code) {
    ostringstream stm;
    stm << "HTTP response code " << response_code;
    return Status::IOError(stm.str());
}
