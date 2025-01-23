#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <ranges>
#include <span>
#include <sstream>
#include <unordered_map>
#include <vector>

#include <fmt/core.h>

#include <report_loads.h>

int main() {
    ReportLoads bs;
    auto filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Berkeley/base/reportloads_wWKTZpn.csv";
    bs.load(filename);
    bs.print_loads();
    ReportLoads bs2;
    filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-4-10/Berkeley/executions/2/results/0_reportloads.csv";
    bs2.load(filename);
    bs2.print_loads();
    //bs.print();
    //std::cout<<"-----------------------------------\n";
    //bs.remove_load_srcs({10,1,12,21});
    //bs.print();

    return 0;
}
