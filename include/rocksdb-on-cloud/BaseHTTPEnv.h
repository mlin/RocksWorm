/*
BaseHTTPEnv: RocksDB Env that reads files over HTTP[S]. Subclasses need to
implement the Env::GetChildren operation, and can override various aspects of
HTTP[S] request formulation (URL rewriting, headers, etc.).
*/

#ifndef BASEHTTPENV_H
#define BASEHTTPENV_H

#include <string>
#include <vector>
#include <unistd.h>
#include "HTTP.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

struct HTTPEnvOptions {

    // HTTP connection pool. If null, the env will create a private connection
    // pool. It could be useful to share connection pools between HTTP Env
    // instances expected to communicate with the same endpoint (e.g.
    // s3.amazonaws.com)
    HTTP::CURLpool *connpool;
    
    // Parameters controlling HTTP retry logic. Connection errors, 5xx
    // response codes, and interrupted requests/responses can be retried.

    // Maximum number of retry attempts (not counting the initial attempt)
    unsigned int retry_times;
    // Microseconds to wait before the first retry attempt
    useconds_t retry_initial_delay;
    // On each subsequent retry, the delay is multiplied by this factor
    unsigned int retry_backoff_factor;

    // stderr log level for HTTP operations. The base HTTP env logs at the
    // following levels:
    //   ERROR  request failures
    //   WARN   retry attempts
    //   INFO   requests and timed responses
    //   DEBUG  all HTTP headers
    rocksdb::InfoLogLevel http_stderr_log_level;

    HTTPEnvOptions()
        : connpool(nullptr)
        , retry_times(4)
        , retry_initial_delay(500000)
        , retry_backoff_factor(2)
        , http_stderr_log_level(rocksdb::InfoLogLevel::WARN_LEVEL)
        {}
};

class StdErrLogger : public rocksdb::Logger {
    std::string fname_;

public:
    StdErrLogger(const std::string& fname, const rocksdb::InfoLogLevel log_level)
        : rocksdb::Logger(log_level)
        , fname_(fname)  {}

    void Logv(const char* format, va_list ap) override {
        fprintf(stderr, "%s ", fname_.c_str());
        vfprintf(stderr, format, ap);
        fprintf(stderr, "\n");
    }

    void LogHeader(const char* format, va_list ap) override {
        // skip excessive detail for read-only uses
    }
};

class BaseHTTPEnv : public rocksdb::Env {
    friend class BaseHTTPRandomAccessFile;
    friend class BaseHTTPSequentialFile;

protected:
    std::string base_url_;
    rocksdb::Env *inner_env_;
    HTTP::CURLpool *connpool_;
    HTTPEnvOptions opts_;
    StdErrLogger http_logger_;

    // Formulate the URL and request headers to HEAD the named file. May be
    // overridden by subclasses to e.g. add authorization headers. The base
    // method just appends fname to base_url.
    virtual rocksdb::Status PrepareHead(const std::string& fname,
                                        std::string& url, HTTP::headers& request_headers);
    // Formulate the URL and request headers to GET the specified byte range
    // of the named file. The base method creates the appropriate HTTP range
    // header.
    virtual rocksdb::Status PrepareGet(const std::string& fname, uint64_t offset, size_t n,
                                       std::string& url, HTTP::headers& request_headers);

    // Perform a HEAD request for the named file, with retry logic
    virtual rocksdb::Status RetryHead(const std::string& fname, HTTP::headers& response_headers);
    // Perform a GET request for the specified range of the name file, with
    // retry logic
    virtual rocksdb::Status RetryGet(const std::string& fname, uint64_t offset, size_t n,
                                     HTTP::headers& response_headers, rocksdb::Slice* response_body, char* scratch);

    // Censor a URL before putting into the log. Subclasses may wish to remove
    // sensitive information.
    virtual std::string CensorURL(const std::string& url) { return url; }
    // Write the headers into the log at DEBUG level. Subclasses may wish to
    // remove sensitive information (e.g. authorization, cookie)
    virtual void LogHeaders(const HTTP::headers& hdrs);

    static rocksdb::Status CURLcodeToStatus(CURLcode c);
    static rocksdb::Status HTTPcodeToStatus(long response_code);

public:
    // The base env will append all requested filenames/paths to base_url
    BaseHTTPEnv(const std::string& base_url, const HTTPEnvOptions& opts);
    virtual ~BaseHTTPEnv();

    // To be overridden by subclasses, as there's no universal way to list a
    // directory over HTTP
    rocksdb::Status GetChildren(const std::string& dir, std::vector<std::string>* result) override {
        return rocksdb::Status::NotSupported("BaseHTTPEnv::GetChildren");
    }

    // The base implementation for GetFileSize performs an HTTP HEAD and
    // returns the content-length
    rocksdb::Status GetFileSize(const std::string& fname, uint64_t* file_size) override;

    // The base implementation of FileExists returns true if GetFileSize
    // succeeds and false otherwise
    rocksdb::Status FileExists(const std::string& fname) override;

    rocksdb::Status NewSequentialFile(const std::string& fname,
                                      std::unique_ptr<rocksdb::SequentialFile>* result,
                                      const rocksdb::EnvOptions& options) override;

    rocksdb::Status NewRandomAccessFile(const std::string& fname,
                                        std::unique_ptr<rocksdb::RandomAccessFile>* result,
                                        const rocksdb::EnvOptions& options) override;

    // Misc unsupported/pass-through methods

    rocksdb::Status GetFileModificationTime(const std::string& fname, uint64_t* file_mtime) override {
        return rocksdb::Status::NotSupported("GetFileModificationTime");
    }

    rocksdb::Status NewWritableFile(const std::string& fname,
                                    std::unique_ptr<rocksdb::WritableFile>* result,
                                    const rocksdb::EnvOptions& options) override {
        return rocksdb::Status::NotSupported("NewWritableFile");
    }

    rocksdb::Status NewRandomRWFile(const std::string& fname,
                                    std::unique_ptr<rocksdb::RandomRWFile>* result,
                                    const rocksdb::EnvOptions& options) override {
        return rocksdb::Status::NotSupported("NewRandomRWFile");
    }

    rocksdb::Status NewDirectory(const std::string& name,
                                 std::unique_ptr<rocksdb::Directory>* result) override {
        return rocksdb::Status::NotSupported("NewDirectory");
    }

    rocksdb::Status DeleteFile(const std::string& fname) override {
        return rocksdb::Status::NotSupported("DeleteFile");
    }    

    rocksdb::Status CreateDir(const std::string& dirname) override {
        return rocksdb::Status::NotSupported("CreateDir");
    }

    rocksdb::Status CreateDirIfMissing(const std::string& dirname) override {
        return rocksdb::Status::NotSupported("CreateDirIfMissing");
    }

    rocksdb::Status DeleteDir(const std::string& dirname) override {
        return rocksdb::Status::NotSupported("DeleteDir");
    }

    rocksdb::Status RenameFile(const std::string& src,
                               const std::string& target) override {
        return rocksdb::Status::NotSupported("RenameFile");
    }

    rocksdb::Status LockFile(const std::string& fname, rocksdb::FileLock** lock) override {
        *lock = nullptr;
        return rocksdb::Status::OK();
    }

    rocksdb::Status UnlockFile(rocksdb::FileLock* lock) override {
        return rocksdb::Status::OK();
    }
  
    void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = 0) override {
        return inner_env_->Schedule(function, arg, pri, tag, unschedFunction);
    }

    int UnSchedule(void* arg, Priority pri) override {
        return inner_env_->UnSchedule(arg, pri);
    }

    void StartThread(void (*function)(void* arg), void* arg) override {
        return inner_env_->StartThread(function, arg);
    }

    void WaitForJoin() override {
        return inner_env_->WaitForJoin();
    }

    unsigned int GetThreadPoolQueueLen(rocksdb::Env::Priority pri = rocksdb::Env::LOW) const override {
        return inner_env_->GetThreadPoolQueueLen(pri);
    }

    rocksdb::Status GetTestDirectory(std::string* path) override {
        return inner_env_->GetTestDirectory(path);
    }

    rocksdb::Status NewLogger(const std::string& fname,
                              std::shared_ptr<rocksdb::Logger>* result) override {
        // note: Db will end up overriding the log level with the one
        // specified in the Db options
        result->reset(new StdErrLogger(fname, opts_.http_stderr_log_level));
        return rocksdb::Status::OK();
    }

    uint64_t NowMicros() override {
        return inner_env_->NowMicros();
    }

    void SleepForMicroseconds(int micros) override {
        return inner_env_->SleepForMicroseconds(micros);
    }

    rocksdb::Status GetHostName(char* name, uint64_t len) override {
        return inner_env_->GetHostName(name, len);
    }

    rocksdb::Status GetCurrentTime(int64_t* unix_time) override {
        return inner_env_->GetCurrentTime(unix_time);
    }

    rocksdb::Status GetAbsolutePath(const std::string& db_path, std::string* output_path) override {
        return inner_env_->GetAbsolutePath(db_path, output_path);
    }

    int GetBackgroundThreads(Priority pri = LOW) override {
        return inner_env_->GetBackgroundThreads(pri);
    }

    void SetBackgroundThreads(int number, rocksdb::Env::Priority pri = LOW) override {
        return inner_env_->SetBackgroundThreads(number, pri);
    }

    void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
        return inner_env_->IncBackgroundThreadsIfNeeded(number, pri);
    }

    std::string TimeToString(uint64_t time) override {
        return inner_env_->TimeToString(time);
    }
};


#endif

