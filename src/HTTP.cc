#include "RocksWorm/HTTP.h"
#include <list>
#include <sstream>
#include <algorithm> 
#include <functional> 
#include <cctype>
#include <locale>
#include <assert.h>

// trim from start
static inline std::string &ltrim(std::string &s) {
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
        return s;
}

// trim from end
static inline std::string &rtrim(std::string &s) {
        s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
        return s;
}

// trim from both ends
static inline std::string &trim(std::string &s) {
        return ltrim(rtrim(s));
}

namespace HTTP {

CURLcode ensure_init() {
    static bool need_init = true;
    if (need_init) {
        CURLcode ans = curl_global_init(CURL_GLOBAL_ALL);
        need_init = false;
        return ans;
    }
    return CURLE_OK;
}

// Helper class for providing HTTP request headers to libcurl
class RequestHeadersHelper {
    std::list<std::string> bufs;
    curl_slist *slist_;
public:
    RequestHeadersHelper(const headers& headers) : slist_(NULL) {
        for(auto it = headers.cbegin(); it != headers.cend(); it++) {
            std::ostringstream stm;
            stm << it->first << ": " << it->second;
            bufs.push_back(stm.str());
            slist_ = curl_slist_append(slist_, bufs.back().c_str());
        }
    }
    virtual ~RequestHeadersHelper() {
        curl_slist_free_all(slist_);
    }
    operator curl_slist*() const { return slist_; }
};

// functions for receiving responses from libcurl
size_t writefunction(char *ptr, size_t size, size_t nmemb, void *userdata) {
    size *= nmemb;
    std::ostream *response_body = reinterpret_cast<std::ostream*>(userdata);
    response_body->write(ptr, size);
    if (response_body->fail()) return 0;
    return size;
}

size_t headerfunction(char *ptr, size_t size, size_t nmemb, void *userdata) {
    size *= nmemb;
    headers& h = *reinterpret_cast<headers*>(userdata);
    
    size_t sep;
    for (sep=0; sep<size; sep++) {
        if (ptr[sep] == ':') break;
    }

    std::string k, v;
    k.assign(ptr, sep);
    k = trim(k);

    if (k.size()) {
        if (sep < size-1) {
            v.assign(ptr+sep+1, size-sep-1);
            v = trim(v);
            if (v.size()) {
                std::transform(k.begin(), k.end(), k.begin(), ::tolower); // lowercase key
                h[k] = v;
            }
        }
    }

    return size;
}

enum class HTTPmethod { GET, HEAD };
// helper macros
#define CURLcall(call) if ((c = call) != CURLE_OK) return c
#define CURLsetopt(x,y,z) CURLcall(curl_easy_setopt(x,y,z))

CURLcode request(HTTPmethod method, const std::string url, const headers& request_headers,
                 long& response_code, headers& response_headers, std::ostream& response_body,
                 CURLpool *pool) {
    CURLcode c;
    CURLcall(ensure_init());

    std::unique_ptr<CURLconn> conn;

    if (pool) {
        conn = pool->checkout();
    } else {
        conn.reset(new CURLconn());
    }

    CURLsetopt(*conn, CURLOPT_URL, url.c_str());

    switch (method) {
    case HTTPmethod::GET:
        CURLsetopt(*conn, CURLOPT_HTTPGET, 1);
        break;
    case HTTPmethod::HEAD:
        CURLsetopt(*conn, CURLOPT_NOBODY, 1);
        break;
    }

    RequestHeadersHelper headers4curl(request_headers);
    CURLsetopt(*conn, CURLOPT_HTTPHEADER, ((curl_slist*) headers4curl));

    CURLsetopt(*conn, CURLOPT_WRITEDATA, &response_body);
    CURLsetopt(*conn, CURLOPT_WRITEFUNCTION, writefunction);

    response_headers.clear();
    CURLsetopt(*conn, CURLOPT_WRITEHEADER, &response_headers);
    CURLsetopt(*conn, CURLOPT_HEADERFUNCTION, headerfunction);

    CURLsetopt(*conn, CURLOPT_FOLLOWLOCATION, 1);
    CURLsetopt(*conn, CURLOPT_MAXREDIRS, 16);

    CURLcall(curl_easy_perform(*conn));

    CURLcall(curl_easy_getinfo(*conn, CURLINFO_RESPONSE_CODE, &response_code));

    if (pool) {
        pool->checkin(conn);
    }

    return CURLE_OK;
}

CURLcode GET(const std::string url, const headers& request_headers,
             long& response_code, headers& response_headers, std::ostream& response_body,
             CURLpool *pool) {
    return request(HTTPmethod::GET, url, request_headers, response_code, response_headers, response_body, pool);
}

CURLcode HEAD(const std::string url, const headers& request_headers,
              long& response_code, headers& response_headers,
              CURLpool *pool) {
    std::ostringstream dummy;
    CURLcode ans = request(HTTPmethod::HEAD, url, request_headers, response_code, response_headers, dummy, pool);
    assert (dummy.str().size() == 0);
    return ans;
}

}
