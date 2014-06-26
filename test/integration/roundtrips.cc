#include <iostream>
#include <sstream>
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/cache.h"
#include "gtest/gtest.h"
#include "test_httpd.h"
#include "rocksdb-on-cloud/RocHTTPEnv.h"
using namespace std;
using namespace rocksdb;

const unsigned short PORT = 18273;

void make_testdb1(string& ans) {
    string DBPATH = "/tmp/roc_integration_tests_roundtrip_small";
    stringstream cmd;
    cmd << "rm -rf " << DBPATH;
    system(cmd.str().c_str());
    
    Status s;
    DB *db = nullptr;
    Options dbopts;
    dbopts.create_if_missing = true;

    s = DB::Open(dbopts,DBPATH,&db);
    assert(s.ok());

    s = db->Put(WriteOptions(), "foo", "Lorem"); ASSERT_TRUE(s.ok());
    s = db->Put(WriteOptions(), "bar", "ipsum"); ASSERT_TRUE(s.ok());
    s = db->Put(WriteOptions(), "bas", "dolor"); ASSERT_TRUE(s.ok());
    s = db->Put(WriteOptions(), "baz", "sit"); ASSERT_TRUE(s.ok());

    s = db->CompactRange(nullptr, nullptr); ASSERT_TRUE(s.ok());
    s = db->Flush(FlushOptions()); ASSERT_TRUE(s.ok());

    delete db;

    ans = DBPATH;
}

int make_roc(const string& dbpath, string& ans) {
    string fn = dbpath + ".roc";
    stringstream cmd;
    cmd << "build/bin/roc " << dbpath << " " << fn;
    ans = fn;
    return system(cmd.str().c_str());
}

TEST(roundtrip, small) {
    string dbpath;
    make_testdb1(dbpath);
    string roc_fn;
    ASSERT_EQ(0,make_roc(dbpath,roc_fn));

    TestHTTPd httpd;
    map<string,string> httpfiles;
    httpfiles["/roc_integration_tests_roundtrip_small.roc"] = roc_fn;
    httpd.Start(PORT,httpfiles);
    
    stringstream localurl;
    localurl << "http://localhost:" << PORT << "/roc_integration_tests_roundtrip_small.roc";
    RocHTTPEnv env(localurl.str(), HTTPEnvOptions());

    Status s;
    DB *db = nullptr;
    Options dbopts;
    ReadOptions rdopts;
    string v;

    dbopts.env = &env;
    dbopts.info_log_level = InfoLogLevel::WARN;

    s = rocksdb::DB::OpenForReadOnly(dbopts,"",&db);
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

    httpd.Stop();
}

static inline uint64_t hash64(uint64_t key)
{
  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ (key >> 24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ (key >> 14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ (key >> 28);
  key = key + (key << 31);
  return key;
}

void make_mediumdb(string& ans) {
    string DBPATH = "/tmp/roc_integration_tests_roundtrip_medium";
    stringstream cmd;
    cmd << "rm -rf " << DBPATH;
    system(cmd.str().c_str());
    
    Status s;
    DB *db = nullptr;
    WriteOptions wropts;
    Options dbopts;
    dbopts.create_if_missing = true;
    dbopts.block_size = 65536;
    dbopts.write_buffer_size = 16*1048576;
    dbopts.target_file_size_base = 64*1048576;
    dbopts.target_file_size_multiplier = 2;

    s = DB::Open(dbopts,DBPATH,&db);
    assert(s.ok());

    const uint64_t M = 1000000;
    for (uint64_t i = 0; i < 25*M; i++) {
        uint64_t hi = __builtin_bswap64(hash64(i));
        s = db->Put(wropts, Slice((const char*)&hi,sizeof(uint64_t)), Slice((const char*)&i,sizeof(uint64_t)));
        ASSERT_TRUE(s.ok());
    }

    s = db->Flush(FlushOptions()); ASSERT_TRUE(s.ok());

    delete db;

    ans = DBPATH;
}

TEST(roundtrip, medium) {
    string dbpath;
    make_mediumdb(dbpath);
    string roc_fn;
    ASSERT_EQ(0,make_roc(dbpath,roc_fn));

    TestHTTPd httpd;
    map<string,string> httpfiles;
    httpfiles["/roc_integration_tests_roundtrip_medium.roc"] = roc_fn;
    httpd.Start(PORT,httpfiles);
    
    stringstream localurl;
    localurl << "http://localhost:" << PORT << "/roc_integration_tests_roundtrip_medium.roc";
    HTTPEnvOptions envopts;
    //envopts.http_stderr_log_level = InfoLogLevel::INFO;
    RocHTTPEnv env(localurl.str(), envopts);
    env.SetBackgroundThreads(4);

    Status s;
    DB *db = nullptr;
    Options dbopts;
    ReadOptions rdopts;
    string v;

    dbopts.env = &env;
    dbopts.info_log_level = InfoLogLevel::WARN;
    dbopts.block_cache = NewLRUCache(1024*1048576); 
    s = rocksdb::DB::OpenForReadOnly(dbopts,"",&db);
    ASSERT_TRUE(s.ok());

    for (uint64_t i = 1000000; i < 1100000; i++) {
        uint64_t hi = __builtin_bswap64(hash64(i));
        ASSERT_TRUE(db->Get(rdopts, Slice((const char*)&hi, sizeof(uint64_t)), &v).ok());
        uint64_t j = *(uint64_t*)v.c_str();
        ASSERT_EQ(i,j);
    }

    Iterator *it = db->NewIterator(rdopts);
    uint64_t h2m = hash64(2000000);
    uint64_t lastkey = 0;
    it->Seek(Slice((const char*)&h2m, sizeof(uint64_t)));
    for (int i = 0; i < 10000; i++) {
        ASSERT_TRUE(it->Valid());
        uint64_t j = *(uint64_t*)it->value().ToString().c_str();
        uint64_t hj = __builtin_bswap64(*(uint64_t*)it->key().ToString().c_str());
        ASSERT_EQ(hash64(j),hj);
        ASSERT_LE(lastkey,hj);
        lastkey = hj;
        it->Next();
    }
    delete it;

    uint64_t h1k[1000];
    vector<Slice> k2;
    vector<string> v2;
    for (uint64_t i = 0; i < 1000; i++) {
        h1k[i] = __builtin_bswap64(hash64(9000000+i));
        k2.push_back(Slice((const char*)&(h1k[i]), sizeof(uint64_t)));
    }
    auto s2 = db->MultiGet(rdopts, k2, &v2);
    for (uint64_t i = 0; i < 1000; i++) {
        ASSERT_TRUE(s2[i].ok());
        ASSERT_EQ(9000000+i,(*(uint64_t*)v2[i].c_str()));
    }

    delete db;

    httpd.Stop();
}

// make a small, universally-compacted DB
void make_univdb(string& ans) {
    string DBPATH = "/tmp/roc_integration_tests_roundtrip_univ";
    stringstream cmd;
    cmd << "rm -rf " << DBPATH;
    system(cmd.str().c_str());
    
    Status s;
    DB *db = nullptr;
    WriteOptions wropts;
    Options dbopts;
    dbopts.create_if_missing = true;
    dbopts.compaction_style = kCompactionStyleUniversal;
    dbopts.write_buffer_size = 4*1048576;

    s = DB::Open(dbopts,DBPATH,&db);
    assert(s.ok());

    for (uint64_t i = 0; i < 1000000; i++) {
        uint64_t hi = __builtin_bswap64(hash64(i));
        s = db->Put(wropts, Slice((const char*)&hi,sizeof(uint64_t)), Slice((const char*)&i,sizeof(uint64_t)));
        ASSERT_TRUE(s.ok());
    }

    s = db->Flush(FlushOptions()); ASSERT_TRUE(s.ok());

    delete db;

    ans = DBPATH;
}

TEST(roundtrip, univ) {
    string dbpath;
    make_univdb(dbpath);
    string roc_fn;
    ASSERT_EQ(0,make_roc(dbpath,roc_fn));

    TestHTTPd httpd;
    map<string,string> httpfiles;
    httpfiles["/roc_integration_tests_roundtrip_univ.roc"] = roc_fn;
    httpd.Start(PORT,httpfiles);
    
    stringstream localurl;
    localurl << "http://localhost:" << PORT << "/roc_integration_tests_roundtrip_univ.roc";
    HTTPEnvOptions envopts;
    //envopts.http_stderr_log_level = InfoLogLevel::INFO;
    RocHTTPEnv env(localurl.str(), envopts);
    env.SetBackgroundThreads(4);

    // We'll read the database without telling RocksDB it's universally
    // compacted - to make sure nothing weird happens

    Status s;
    DB *db = nullptr;
    Options dbopts;
    ReadOptions rdopts;
    string v;

    dbopts.env = &env;
    dbopts.info_log_level = InfoLogLevel::WARN;
    dbopts.block_cache = NewLRUCache(1024*1048576); 
    s = rocksdb::DB::OpenForReadOnly(dbopts,"",&db);
    ASSERT_TRUE(s.ok());

    Iterator *it = db->NewIterator(rdopts);
    uint64_t lastkey = 0;
    it->SeekToFirst();
    for (int i = 0; i < 1000000; i++) {
        ASSERT_TRUE(it->Valid());
        uint64_t j = *(uint64_t*)it->value().ToString().c_str();
        uint64_t hj = __builtin_bswap64(*(uint64_t*)it->key().ToString().c_str());
        ASSERT_EQ(hash64(j),hj);
        ASSERT_LE(lastkey,hj);
        lastkey = hj;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;

    for (uint64_t i = 0; i < 1000000; i++) {
        uint64_t hi = __builtin_bswap64(hash64(i));
        ASSERT_TRUE(db->Get(rdopts, Slice((const char*)&hi, sizeof(uint64_t)), &v).ok());
        uint64_t j = *(uint64_t*)v.c_str();
        ASSERT_EQ(i,j);
    }

    delete db;

    httpd.Stop();
}

TEST(roundtrip, retry) {
    string dbpath;
    make_testdb1(dbpath);
    string roc_fn;
    ASSERT_EQ(0,make_roc(dbpath,roc_fn));

    TestHTTPd httpd;
    map<string,string> httpfiles;
    httpfiles["/roc_integration_tests_roundtrip_retry.roc"] = roc_fn;
    httpd.Start(PORT,httpfiles);

    stringstream localurl;
    localurl << "http://localhost:" << PORT << "/roc_integration_tests_roundtrip_retry.roc";
    HTTPEnvOptions envopts;
    envopts.http_stderr_log_level = InfoLogLevel::INFO;
    RocHTTPEnv env(localurl.str(), envopts);

    Status s;
    DB *db = nullptr;
    Options dbopts;
    ReadOptions rdopts;
    string v;

    dbopts.env = &env;
    dbopts.info_log_level = InfoLogLevel::WARN;

    httpd.FailNextRequests(1);
    s = rocksdb::DB::OpenForReadOnly(dbopts,"",&db);
    ASSERT_TRUE(s.ok());

    httpd.FailNextRequests(3);
    s = db->Get(rdopts, Slice("foo"), &v);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(string("Lorem"),v);

    httpd.FailNextRequests(1);
    s = db->Get(rdopts, Slice("bogus"), &v);
    ASSERT_TRUE(s.IsNotFound());

    delete db;

    httpd.Stop();
}
