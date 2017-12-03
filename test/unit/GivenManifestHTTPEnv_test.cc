#include <iostream>
#include <sstream>
#include "gtest/gtest.h"
#include "rocksdb-on-cloud/GivenManifestHTTPEnv.h"
using namespace std;
using namespace rocksdb;

TEST(GivenManifestHTTPEnv, SimpleOps) {
    const uint64_t sz = 1024;
    GivenManifestHTTPEnv::manifest manifest;
    manifest["/index.html"] = sz;
    manifest["/foo"] = sz*2;
    GivenManifestHTTPEnv env("http://www.mlin.net", manifest, HTTPEnvOptions());

    ASSERT_TRUE(env.FileExists("/index.html").ok());
    ASSERT_FALSE(env.FileExists("/bar").ok());

    uint64_t reported_sz = 0;
    Status s = env.GetFileSize("/index.html", &reported_sz);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(sz, reported_sz);

    s = env.GetFileSize("/bar", &reported_sz);
    ASSERT_TRUE(s.IsNotFound());

    vector<string> children;
    s = env.GetChildren("/", &children);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(2, children.size());
    ASSERT_EQ("/index.html", children[1]);
    ASSERT_EQ("/foo", children[0]);

    s = env.GetChildren("/foo", &children);
    ASSERT_TRUE(s.IsNotFound());
}

TEST(GivenManifestHTTPEnv, Read) {
    const uint64_t sz = 1024;
    GivenManifestHTTPEnv::manifest manifest;
    manifest["1000genomes/README.alignment_data"] = sz;
    manifest["1000genomes/BOGUS"] = 1;
    GivenManifestHTTPEnv env("http://s3.amazonaws.com", manifest, HTTPEnvOptions());

    // read sz bytes
    unique_ptr<SequentialFile> f;
    Status s = env.NewSequentialFile("1000genomes/README.alignment_data", &f, EnvOptions());
    ASSERT_TRUE(s.ok());

    char buf[sz];
    Slice data;
    s = f->Read(sz, &data, buf);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(sz, data.size());

    // read two consecutive chunks of sz/2 bytes each
    s = env.NewSequentialFile("1000genomes/README.alignment_data", &f, EnvOptions());
    ASSERT_TRUE(s.ok());

    char buf1[sz], buf2[sz];
    Slice data1, data2;
    s = f->Read(sz/2, &data1, buf1);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(sz/2, data1.size());
    s = f->Read(sz/2, &data2, buf2);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(sz/2, data2.size());

    // verify the two chunks combine to the whole
    ASSERT_TRUE(data.starts_with(data1));
    ASSERT_FALSE(data.starts_with(data2));

    data.remove_prefix(sz/2);
    ASSERT_EQ(0, data.compare(data2));

    // reads beyond known EOF
    s = f->Read(sz, &data, buf);
    ASSERT_EQ(0, data.size());
    s = env.NewSequentialFile("1000genomes/README.alignment_data", &f, EnvOptions());
    ASSERT_TRUE(s.ok());
    s = f->Read(sz/2, &data, buf);
    ASSERT_TRUE(s.ok());
    s = f->Read(sz, &data, buf);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(sz/2, data.size());
}

TEST(GivenManifestHTTPEnv, HTTPError) {
    GivenManifestHTTPEnv::manifest manifest;
    manifest["BOGUS"] = 1;
    HTTPEnvOptions envopts;
    envopts.http_stderr_log_level = InfoLogLevel::INFO_LEVEL;
    GivenManifestHTTPEnv env("http://www.google.com", manifest, envopts);

    unique_ptr<SequentialFile> f;
    char buf;
    Slice data;
    Status s = env.NewSequentialFile("BOGUS", &f, EnvOptions());
    ASSERT_TRUE(s.ok());
    s = f->Read(1, &data, &buf);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ("IO error: HTTP response code 404",s.ToString());
}

TEST(GivenManifestHTTPEnv, CurlError) {
    GivenManifestHTTPEnv::manifest manifest;
    manifest["BOGUS"] = 1;
    HTTPEnvOptions opts;
    opts.retry_times = 1;
    opts.http_stderr_log_level = InfoLogLevel::INFO_LEVEL;
    GivenManifestHTTPEnv env("http://www.notarealdomain194851.com", manifest, opts);

    unique_ptr<SequentialFile> f;
    char buf;
    Slice data;
    Status s = env.NewSequentialFile("BOGUS", &f, EnvOptions());
    ASSERT_TRUE(s.ok());
    s = f->Read(1, &data, &buf);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ("IO error: Couldn't resolve host name",s.ToString());
}

TEST(GivenManifestHTTPEnv, testdb1) {
    GivenManifestHTTPEnv::manifest manifest;
    manifest["4e32de754389b819d8569c84604653d01859bd564f788be8fabb657412da3d93/IDENTITY"] = 37;
    manifest["4e32de754389b819d8569c84604653d01859bd564f788be8fabb657412da3d93/CURRENT"] = 16;
    manifest["4e32de754389b819d8569c84604653d01859bd564f788be8fabb657412da3d93/MANIFEST-000004"] = 145;
    manifest["4e32de754389b819d8569c84604653d01859bd564f788be8fabb657412da3d93/000007.sst"] = 521;
    GivenManifestHTTPEnv env("https://github.com/mlin/rocksdb-on-cloud/raw/master/test/data/", manifest, HTTPEnvOptions());

    Status s;
    DB *db = nullptr;
    Options dbopts;
    ReadOptions rdopts;
    string v;

    dbopts.env = &env;
    dbopts.info_log_level = InfoLogLevel::WARN_LEVEL;

    s = rocksdb::DB::OpenForReadOnly(dbopts,"4e32de754389b819d8569c84604653d01859bd564f788be8fabb657412da3d93",&db);
    ASSERT_TRUE(s.ok());

    s = db->Get(rdopts, Slice("foo"), &v);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(string("Lorem"),v);

    s = db->Get(rdopts, Slice("bas"), &v);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(string("dolor"),v);

    s = db->Get(rdopts, Slice("bogus"), &v);
    ASSERT_TRUE(s.IsNotFound());

    map<string,string> m;
    Iterator *it = db->NewIterator(rdopts);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    while (it->Valid()) {
        m[it->key().ToString()] = it->value().ToString();
        //cout << it->key().ToString() << "\t" << it->value().ToString() << endl;
        it->Next();
    }
    ASSERT_TRUE(it->status().ok());
    ASSERT_EQ(4,m.size());
    ASSERT_EQ(string("ipsum"),m["bar"]);
    ASSERT_EQ(string("dolor"),m["bas"]);
    ASSERT_EQ(string("sit"),m["baz"]);
    ASSERT_EQ(string("Lorem"),m["foo"]);
    delete it;

    vector<Slice> k2;
    vector<string> v2;
    k2.push_back(Slice("foo"));
    k2.push_back(Slice("bas"));
    auto s2 = db->MultiGet(rdopts, k2, &v2);
    ASSERT_TRUE(s2[0].ok());
    ASSERT_TRUE(s2[1].ok());
    ASSERT_EQ(string("Lorem"),v2[0]);
    ASSERT_EQ(string("dolor"),v2[1]);

    delete db;
}
