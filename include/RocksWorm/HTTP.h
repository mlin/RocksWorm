/** Wrappers for libcurl HTTP operations */
#pragma once

#include <iostream>
#include <string>
#include <map>
#include <memory>
#include <queue>
#include <mutex>
#include <curl/curl.h>

namespace HTTP {

// Helper class to scope a CURL handle
class CURLconn {
    CURL *h_;
public:
    CURLconn() : h_(NULL) {
        h_ = curl_easy_init();
    }
    virtual ~CURLconn() {
        if (h_) curl_easy_cleanup(h_);
    }
    operator CURL*() const { return h_; }
};

// A very simple pool of CURL handles, which can persist server connections in
// between requests. Any number of handles can be checked out; at most 'size'
// handles will be kept once checked back in. (Since we use blocking
// operations, 'size' should probably be set to the number of threads that
// could make concurrent requests)
class CURLpool {
    unsigned int size_;
    std::queue<std::unique_ptr<CURLconn>> pool_;
    std::mutex mu_;

public:
    CURLpool(const unsigned int size)
        : size_(size) {}

    std::unique_ptr<CURLconn> checkout() {
        std::lock_guard<std::mutex> lock(mu_);
        std::unique_ptr<CURLconn> ans;
        if (pool_.empty()) {
            ans.reset(new CURLconn());
        } else {
            ans.reset(pool_.front().release());
            pool_.pop();
        }
        return ans;
    }

    void checkin(std::unique_ptr<CURLconn>& p) {
        std::lock_guard<std::mutex> lock(mu_);
        if (pool_.size() < size_) {
            pool_.push(std::move(p));
        } else {
            p.reset();
        }
    }
};

using headers = std::map<std::string,std::string>;

CURLcode GET(const std::string url, const headers& request_headers,
             long& response_code, headers& response_headers, std::ostream& response_body,
             CURLpool *pool = nullptr);

CURLcode HEAD(const std::string url, const headers& request_headers,
              long& response_code, headers& response_headers,
              CURLpool *pool = nullptr);

}
