
//
// Created by Gregorio Toscano on 3/27/23.
//

#ifndef MANURE_SCENARIO_H
#define MANURE_SCENARIO_H

#include "csv.hpp"
#include "data_reader.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>


struct ManureStruct {
    int bmp;
    int agency;
    int state;
    int county_from;
    int county_to;
    std::string fips_from;
    std::string fips_to;
    int animal_grp;
    int load_src_grp;
    int unit;
    double amount;
    double cost;
};

struct ManureSol {
    int bmp;
    int agency;
    int state;
    int county_from;
    int county_to;
    std::string fips_from;
    std::string fips_to;
    int animal_grp;
    int load_src_grp;
    int unit;
    double amount;
};


class ManureScenario {
public:

    ManureScenario(const DataReader& data_reader);
    void set_data(const DataReader& data_reader);

    int write(const std::vector<ManureSol>& rows, const std::string& out_filename);
    void load_scenario(const std::string& filename);
    std::vector<ManureSol> get_scenario();
    double compute_cost(int cost_profile_id);
    double compute_cost(const std::string& filename, int cost_profile_id);
private:
    std::vector<ManureSol> scenario_;
    const DataReader& data_;
};


#endif //MANURE_SCENARIO_H
