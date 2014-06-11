#ifndef TEST_HTTPD_H
#define TEST_HTTPD_H

#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <microhttpd.h>

#include <string>
#include <map>
#include <memory>

class TestHTTPd {
	unsigned short port_;
	std::map<std::string,std::string> files_;
	MHD_Daemon *d_;

	friend int on_request(void *cls, struct MHD_Connection *connection,
                     const char *url, const char *method,
                     const char *version, const char *upload_data,
                     size_t *upload_data_size, void **con_cls);

	int OnRequest(MHD_Connection *connection,
                     const char *url, const char *method,
                     const char *version, const char *upload_data,
                     size_t *upload_data_size, void **con_cls);

public:
	TestHTTPd() : d_(nullptr) {}
	virtual ~TestHTTPd();

	bool Start(unsigned short port, const std::map<std::string,std::string>& files);
	void Stop();
};

#endif