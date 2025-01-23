//
// Created by Gregorio Toscano on 3/27/23.
//

#ifndef LAND_SCENARIO_H
#define LAND_SCENARIO_H

#include "csv.hpp"
#include "data_reader.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>


struct ScenarioStruct {
    int agency;
    int state;
    int bmp;
    int geo;
    int load_src_grp;
    int unit;
    double amount;
    double cost;
    int lrseg;
    int load_src;
    double alpha;
    double capital;
    double nitrogen;
    double phosphorus;
    double sediments;
};

struct LandSol {
    int lrseg;
    int agency;
    int load_src;
    double amount;
    int bmp;
};

class LandScenario {
public:
    LandScenario(const DataReader& data_reader);
    void set_data(const DataReader& data_reader);
    int write(
            const std::vector<ScenarioStruct>& rows,
            const std::string& output_path,
            const std::string& exec_uuid
    );

    int write(
            const std::string& output_path,
            const std::string& exec_uuid
    );

    int write_land(
            const std::vector<LandSol>& rows,
            const std::string& out_filename
    );
    void load_land_scenario(const std::string& filename);
    std::vector<LandSol> get_land_scenario();
    LandSol prepend(std::string key, int bmp, double amount);
    double compute_cost(int cost_profile_id);
    double compute_cost(const std::string& loads_filename, int cost_profile_id);
private:
    std::vector<LandSol> land_scenario;
    const DataReader& data_;
};


#endif //LAND_SCENARIO_H
