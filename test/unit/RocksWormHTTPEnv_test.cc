#include <iostream>
#include <sstream>
#include "gtest/gtest.h"
#include "RocksWorm/RocksWormHTTPEnv.h"
using namespace std;
using namespace rocksdb;

TEST(RocksWormHTTPEnv, testdb1) {
    RocksWormHTTPEnv env("https://github.com/mlin/rocksdb-on-cloud/raw/master/test/data/4e32de754389b819d8569c84604653d01859bd564f788be8fabb657412da3d93/testdb1.roc", HTTPEnvOptions());

    Status s;
    DB *db = nullptr;
    Options dbopts;
    ReadOptions rdopts;
    string v;

    dbopts.env = &env;
    dbopts.info_log_level = InfoLogLevel::WARN_LEVEL;

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
}
