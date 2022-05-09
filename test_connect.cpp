#include <cstdlib>
#include <iostream>
#include <thread>
#include <chrono>
#include "duckdb.hpp"

using namespace std;
using namespace duckdb;

std::string random_string( size_t length) {
     std::string str("abcdefghijklmnopqrstuvwxyz");

     std::random_shuffle(str.begin(), str.end());

     return str.substr(0, length);
}

void CreateAndLoadTable(Connection& conn, int rowCount) {
    conn.Query("Create Table test(c1 integer, c2 varchar)");
    Appender appender(conn, "test");
    for(int i = 0; i < rowCount; i++) {
        appender.AppendRow(i + 1, random_string(10).c_str());
    }
    appender.Flush();
    appender.Close();
}

void TestDuckDB(int rowCount) {
    DuckDB db(nullptr);
    Connection conn(db);
    CreateAndLoadTable(conn, rowCount);
    auto result = conn.Query("Select * from test order by c2");
    result->Print();
}

int GetDbCount(int argc, char** argv) {
    if(argc > 1) {
        return std::atoi(argv[1]);
    }
    return 10;
}

int GetRowCount(int argc, char** argv) {
    if(argc > 2) {
        return std::atoi(argv[2]);
    }
    return 10;
}

int main(int argc, char** argv) {

    int dbCount = GetDbCount(argc, argv);
    int rowCount = GetRowCount(argc, argv);

    for(int i = 0; i < dbCount; i++) {
        cout << "Creating DB instance # " << (i+1) << std::endl;
        TestDuckDB(rowCount);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}

