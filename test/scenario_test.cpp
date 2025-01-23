//
// Created by gtoscano on 4/15/23.
//

#include "scenario.h"
#include <iostream>
#include <string>
#include <fmt/core.h>
//For Jefferson County: ./scenario_test 3814 report_loads_Jefferson.csv scenario_Jefferson.json

int main(int argc, char** argv) {
    Scenario scenario;

    if (argc < 4) {
        std::cout << "Usage: " << argv[0] << " <scenario_id> <input_file> <output_file>" << std::endl;
        return 1;
    }
    size_t scenario_id = std::stoi(argv[1]); 
    std::string filename = argv[2]; 
    std::string out_filename = argv[3];
    fmt::print("Scenario ID: {}\n", scenario_id);
    fmt::print("Input file: {}\n", filename);
    fmt::print("Output file: {}\n", out_filename);

	//WIP3 Official: 3858 7	60	1	5316		128	0	59	31	6	4	13	0	1		1	0
    auto WIP3= 3858;
    //WIP3 2019:     3817 8	262	1	6611	128	0	59	31	6	4	13	0	1		1	0
    auto WIP3_2019 = 3817; //3817	WIP 3 CAST-2019 version	BMPs from the major jurisdictions' Final Phase 3 WIPs run in the CAST-2019 version. The official version was run in the CAST-2017d version.	
    //Progress 2019: 8	256	2	6611		158	0	59	31	6	4	13	0	1		1	0
    auto Progress_2019= 3814;//	2019 Progress	Reflects the BMPs that are functioning in this year, as reported by the state to the Chesapeake Bay Program for annual progress. Uses 2019 base conditions and assumes maximum feasible air reductions are already considered.	
    // Progress 2017: 8	254	2	6611		42	0	59	31	6	4	13	0	1		1	0
    auto Progress_2017 =3812; //3812	2017 Progress	Reflects the BMPs that are functioning in this year, as reported by the state to the Chesapeake Bay Program for annual progress. Uses 2017 base conditions and assumes maximum feasible air reductions are already considered.	
    //scenario_id = Progress_2019;
    scenario.create_scenario(scenario_id, filename, out_filename);
    /*
    Scenario scenario2;
    scenario2.load(out_filename);
    scenario2.save("test2.json");
    */
}

