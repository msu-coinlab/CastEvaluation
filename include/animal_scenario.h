//
// Created by Gregorio Toscano on 3/27/23.
//

#ifndef ANIMAL_SCENARIO_H
#define ANIMAL_SCENARIO_H

#include "csv.hpp"
#include "data_reader.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>


struct AnimalStruct {
    int bmp;
    int agency;
    int state;
    int geo;
    int animal_grp;
    int load_src_grp;
    int unit;
    double amount;
    double cost;
    int load_src;
};

struct AnimalSol {
    int bmp;
    int agency;
    int state;
    int geo;
    int animal_grp;
    int load_src_grp;
    int unit;
    double amount;
    int load_src;
};

class AnimalScenario {
public:
    AnimalScenario(const DataReader& data_reader);
    void set_data(const DataReader& data_reader);

    int write2 (
            const std::vector<AnimalSol>& rows,
            const std::string& out_filename
    );
    int write (
            const std::vector<AnimalSol>& rows,
            const std::string& out_filename
    );
    void load_scenario(const std::string& filename);
    std::vector<AnimalSol> get_scenario();
    double compute_cost(int cost_profile_id);
    double compute_cost(const std::string& filename, int cost_profile_id);
private:
    std::vector<AnimalSol> scenario_;
    const DataReader& data_;
};


#endif //ANIMAL_SCENARIO_H
