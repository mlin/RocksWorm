#include "test_httpd.h"
#include <iostream>
#include <sstream>
#include <fstream>
using namespace std;

int on_request(void *cls, struct MHD_Connection *connection,
                 const char *url, const char *method,
                 const char *version, const char *upload_data,
                 size_t *upload_data_size, void **con_cls)
{
    TestHTTPd *d = reinterpret_cast<TestHTTPd*>(cls);
    return d->OnRequest(connection, url, method, version, upload_data, upload_data_size, con_cls);
}

TestHTTPd::~TestHTTPd() {
    Stop();
}

bool TestHTTPd::Start(unsigned short port, const map<string,string>& files) {
    if (d_) {
        cerr << "TestHTTPd::Start: daemon already running" << endl;
        return false;
    }

    port_ = port;
    files_ = files;
    d_ = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY | MHD_USE_THREAD_PER_CONNECTION,
                          port, nullptr, nullptr, &on_request, this, MHD_OPTION_END);

    if (!d_) {
        cerr << "TestHTTPd::Start: MHD_start_daemon failed" << endl;
        return false;
    }

    return true;
}

void TestHTTPd::Stop() {
    if (d_) {
        MHD_stop_daemon(d_);
        d_ = nullptr;
    }
}

bool get_range_header(MHD_Connection *connection, size_t& lo, size_t& hi) {
    const char* crangehdr = MHD_lookup_connection_value(connection, MHD_HEADER_KIND, "range");
    if (!crangehdr) return false;
    string rangehdr(crangehdr);
    if (rangehdr.size() < 9 || rangehdr.substr(0,6) != "bytes=") return false;
    size_t dashpos = rangehdr.find('-');
    if (dashpos == string::npos || rangehdr.rfind('-') != dashpos || dashpos < 7 || dashpos == rangehdr.size()-1) return false;
    string slo = rangehdr.substr(6,dashpos-6);
    string shi = rangehdr.substr(dashpos+1);
    lo = strtoull(slo.c_str(), nullptr, 10);
    hi = strtoull(shi.c_str(), nullptr, 10);
    return true;
}

int TestHTTPd::OnRequest(MHD_Connection *connection,
                         const char *url, const char *method,
                         const char *version, const char *upload_data,
                         size_t *upload_data_size, void **con_cls) {
    MHD_Response *response = nullptr;
    unsigned int response_code = 404;
    int ret;

    if (requests_to_fail_ == 0) {
        auto entry = files_.find(url);
        if (entry != files_.end()) {
            int fd = open(entry->second.c_str(), O_RDONLY);
            if (fd > 0) {
                struct stat st;
                if (fstat(fd,&st) == 0) {
                    size_t lo, hi;
                    if (get_range_header(connection,lo,hi)) {
                        //cout << lo << " - " << hi << endl;
                        if (hi >= lo && lo < size_t(st.st_size) && hi < size_t(st.st_size)) {
                            response_code = 206;
                            // TODO set content-range response header, "lo-hi/st.st_size"
                            if (!(response = MHD_create_response_from_fd_at_offset(hi-lo+1, fd, lo))) return MHD_NO;
                        } else response_code = 416;
                    } else {
                        response_code = 200;
                        if (!(response = MHD_create_response_from_fd(st.st_size, fd))) return MHD_NO;
                    }
                } else response_code = 500;
            }
        }
    } else {
        response_code = 500;
        --requests_to_fail_;
    }

    if (response == nullptr) {
        if (!(response = MHD_create_response_from_buffer(0, (void*) "", MHD_RESPMEM_PERSISTENT))) return MHD_NO;
    }
    ret = MHD_queue_response(connection, response_code, response);
    MHD_destroy_response(response);

    return ret;
}
