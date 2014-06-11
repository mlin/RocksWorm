#include <iostream>
#include <sstream>
#include "gtest/gtest.h"
#include "rocksdb-on-cloud/HTTP.h"
using namespace std;

TEST(HTTP, mlin_net) {
    HTTP::headers request_headers, response_headers;
    long response_code = -1;
    ostringstream response_body_stream;

    CURLcode c = HTTP::GET("http://www.mlin.net/", request_headers,
                           response_code, response_headers, response_body_stream);

    ASSERT_EQ(CURLE_OK, c);
    ASSERT_EQ(200, response_code);
    ASSERT_NE(std::string::npos, response_headers.at("content-type").find("text/html"));

    string response_body = response_body_stream.str();
    ASSERT_NE(std::string::npos, response_body.find("Mike Lin"));
}

TEST(HTTP, HEAD_mlin_net) {
    HTTP::headers request_headers, response_headers;
    long response_code = -1;

    CURLcode c = HTTP::HEAD("http://www.mlin.net/", request_headers,
                            response_code, response_headers);

    ASSERT_EQ(CURLE_OK, c);
    ASSERT_EQ(200, response_code);
    ASSERT_NE(std::string::npos, response_headers.at("content-type").find("text/html"));
}

TEST(HTTP, https_google) {
    HTTP::headers request_headers, response_headers;
    long response_code = -1;
    ostringstream response_body_stream;

    CURLcode c = HTTP::GET("https://www.google.com/", request_headers,
                           response_code, response_headers, response_body_stream);

    ASSERT_EQ(CURLE_OK, c);
    ASSERT_EQ(200, response_code);
    ASSERT_NE(std::string::npos, response_headers.at("content-type").find("text/html"));

    string response_body = response_body_stream.str();
    ASSERT_NE(std::string::npos, response_body.find("Google"));
}

TEST(HTTP, bogus_url) {
    HTTP::headers request_headers, response_headers;
    long response_code = -1;
    ostringstream response_body_stream;

    CURLcode c = HTTP::GET("http://asdf/", request_headers,
                           response_code, response_headers, response_body_stream);

    ASSERT_NE(CURLE_OK, c);
}
