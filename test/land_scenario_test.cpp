//
// Created by gtoscano on 3/31/23.
//

#include "land_scenario.h"
#include "animal_scenario.h"
#include "manure_scenario.h"
#include "data_reader.h"
#include "amqp.h"
#include "misc_utilities.h"
#include "report_loads.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <crossguid/guid.hpp>
#include <string>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>
#include <fmt/core.h>
#include <filesystem>
#include <iostream>
namespace po = boost::program_options;
namespace fs = std::filesystem;

void mkdir(std::string dirPath) {

    if (!fs::exists(dirPath)) {
        if (fs::create_directory(dirPath)) {
            std::cout << "Directory created successfully." << std::endl;
        } else {
            std::cerr << "Failed to create directory." << std::endl;
        }
    } else {
        std::cout << "Directory already exists. Doing nothing." << std::endl;
    }
}


int count_numbers(const std::string& str) {
    if (str.empty()) return 0;

    int count = 1; // Start at 1 to account for the last number
    for (char ch : str) {
        if (ch == '_') {
            count++;
        }
    }
    return count;
}

void my_test(const std::string& land_filename, const std::string& animal_filename, const std::string& manure_filename, const std::string& reportloads_filename, const std::string& counties, std::string emo_uuid, std::string exec_uuid) {
    double total_cost = 0.0;

    std::string emo_path = fmt::format("/opt/opt4cast/output/nsga3/{}/", emo_uuid);
    mkdir(emo_path);
    //exec_uuid = "ceeff93d-724f-4431-bc5d-c564ff3af250";
    fmt::print("emo_uuid: {}\nexec_uuid: {}\n", emo_uuid, exec_uuid);

    //std::string filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Berkeley/base/reportloads_wWKTZpn.csv";
    //filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Jefferson/base/reportloads_kjVfBNe.csv";
    ReportLoads base_scenario;
    base_scenario.load(reportloads_filename);

    if (fs::exists(reportloads_filename)) {

        auto out_filename = fmt::format("{}/{}_reportloads.csv", emo_path, exec_uuid);
        misc_utilities::copy_file(reportloads_filename, out_filename);
    }
    //auto scenario_path = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Berkeley/executions/0/results/first/test";
    //auto scenario_path = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-4-10/Berkeley/executions/2/results/";
    auto exec_id = "0";
    DataReader data_reader;
    data_reader.read_all();
    //auto scen_filename = fmt::format("{}/{}_output_t.csv", scenario_path, exec_id);
    bool land_bool = false;
    bool animal_bool = false;
    bool manure_bool = false;
    if (fs::exists(land_filename)) {
        LandScenario cf(data_reader);
        if(misc_utilities::is_parquet_file(land_filename)) {
            auto par_filename = fmt::format("{}/{}_impbmpsubmittedland.parquet", emo_path, exec_uuid);
            misc_utilities::copy_file(land_filename, par_filename);
            auto cost_profile_id = 7;
            auto total = cf.compute_cost(land_filename, cost_profile_id);
            total_cost += total;
    
            fmt::print("Total Land Cost: {}\n", total);
            land_bool = true;
        } else {

            cf.load_land_scenario(land_filename);
            auto scen = cf.get_land_scenario();
            auto par_filename = fmt::format("{}/{}_impbmpsubmittedland.parquet", emo_path, exec_uuid);
            cf.write_land(scen, par_filename);
            auto cost_profile_id = 7;
            auto total = cf.compute_cost(cost_profile_id);
            total_cost += total;
    
            fmt::print("Total Land Cost: {}\n", total);
            land_bool = true;
        }
    }

    if (fs::exists(animal_filename)) {
        AnimalScenario animal(data_reader);
        if(misc_utilities::is_parquet_file(animal_filename)) {
            auto par_filename = fmt::format("{}/{}_impbmpsubmittedanimal.parquet", emo_path, exec_uuid);
            misc_utilities::copy_file(animal_filename, par_filename);
            auto cost_profile_id = 7;
            auto total = animal.compute_cost(animal_filename, cost_profile_id);
            total_cost += total;
            fmt::print("Total Animal Cost: {}\n", total);
            animal_bool = true;
        } else {
            animal.load_scenario(animal_filename);
            auto animal_scen = animal.get_scenario();
            //auto source = fmt::format("impbmpsubmittedanimal.parquet", emo_path, exec_uuid);
            auto destination = fmt::format("{}/{}_impbmpsubmittedanimal.parquet", emo_path, exec_uuid);
            animal.write(animal_scen, destination);
            auto cost_profile_id = 7;
            auto total = animal.compute_cost(cost_profile_id);
            total_cost += total;
            fmt::print("Total Animal Cost: {}\n", total);
            animal_bool = true;
        }
        //misc_utilities::copy_file(source, destination);
    }

    if (fs::exists(manure_filename)) {
        ManureScenario manure(data_reader);
        if(misc_utilities::is_parquet_file(manure_filename)) {
            auto par_filename = fmt::format("{}/{}_impbmpsubmittedmanuretransport.parquet", emo_path, exec_uuid);
            misc_utilities::copy_file(manure_filename, par_filename);
            auto cost_profile_id = 7;
            auto total = manure.compute_cost(manure_filename, cost_profile_id);
            total_cost += total;
            fmt::print("Total manure Cost: {}\n", total);
            manure_bool = true;
        } else {
            manure.load_scenario(manure_filename);
            auto manure_scen = manure.get_scenario();
            //auto source = fmt::format("impbmpsubmittedmanure.parquet", emo_path, exec_uuid);
            auto destination = fmt::format("{}/{}_impbmpsubmittedmanuretransport.parquet", emo_path, exec_uuid);
            manure.write(manure_scen, destination);
            auto cost_profile_id = 7;
            auto total = manure.compute_cost(cost_profile_id);
            total_cost += total;
            fmt::print("Total manure Cost: {}\n", total);
            manure_bool = true;
        }
        //misc_utilities::copy_file(source, destination);
    }

    fmt::print("Grand Total Cost: {}\n", total_cost);

    if (land_bool ==false && animal_bool == false && manure_bool == false) {
        fmt::print("No land, animal or manure files\nNo data will be process. Exiting now....\n");
        return;
    }


    //std::string emo_data = "empty_38_6611_256_6_8_59_1_6608_36_2_31_8_410";
    //36 = No Action
    //158 = 2019
    //137 = WIP 3
    int atm_dep_data_set = 38;
    int back_out_scenario = 6611;
    int base_condition = 256;
    int base_load = 6;
    int cost_profile = 7;
    int climate_change_data_set = 59;
    int n_counties = 1;
    int historical_crop_need_scenario = 6608;
    int point_source_data_set = 158; //it was 36 for all our previous executions; //158 = 2019
    //point_source_data_set = 36;
    int scenario_type = 2;
    int soil_p_data_set = 31;
    int source_data_revision = 8;
    //  #std::string counties = "332";//Lancaster
    n_counties = count_numbers(counties);
    /*
    counties = "410";
    counties = "381_364_402_391_362_367_365";
    counties = "381";//Nelson
    */
    //counties = "364";//Amherst
    //counties = "391";//Amherst
    //empty_38_6611_256_6_4_59_1_6608_158_2_31_8_381

    std::string emo_str = fmt::format("empty_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}",
          atm_dep_data_set, //1
          back_out_scenario, //2
          base_condition, //3
          base_load, //4
          cost_profile, //5
          climate_change_data_set, //6
          n_counties, //7
          historical_crop_need_scenario, //8
          point_source_data_set, //9
          scenario_type, //10
          soil_p_data_set, //11
          source_data_revision, //12
          counties //13
          );
    //                       0     1   2   3  4 5 6  7  8    9  10 11 12 13
    std::cout<<"emo_str: "<<emo_str<<std::endl;
    //std::string emo_data = "empty_38_6611_256_6_8_59_1_6608_158_2_31_8_410";
    //emo_data = "empty_38_6611_256_6_7_59_4_6608_158_2_31_8_362_391_403_404";
    //emo_data = "empty_38_6611_256_6_7_59_1_6608_158_2_31_8_381";
    //std::cout<<"emo_data: "<<emo_data<<std::endl;
    //point source dataset 158

    //emo_uuid = "b3795dee-e704-4268-ad5e-531c9f911e3f";

    RabbitMQClient rabbit(emo_str, emo_uuid);

    rabbit.send_signal(exec_uuid);

    auto output_str = rabbit.wait_for_data();
    std::vector<std::string> loads;
    misc_utilities::split_str(output_str, '_', loads);
    fmt::print("N: {}, P: {}, S: {}\n",loads[0], loads[1], loads[2]);

    ReportLoads base_scenario2;
    auto filename = fmt::format("{}/{}_reportloads.csv", emo_path, exec_uuid);
    base_scenario2.load(filename);
    fmt::print("Original Loads: \n");
    base_scenario.print_loads();
    fmt::print("New Loads: \n");
    base_scenario2.print_loads();
}

/* Example of calling it:
 *
 *
 * */
int main(int argc, char* argv[]) {
    std::string land_filename = "";
    std::string reportloads_filename = "";
    std::string animal_filename = "";
    std::string manure_filename = "";
    std::string counties = "";
    std::string uuid = "";
    po::options_description desc("Allowed options");

    desc.add_options()
        ("help,h", "produce help message")
        ("land,l", po::value<std::string>(), "land scenario file")
        ("animal,a", po::value<std::string>(), "animal scenario file")
        ("manure,m", po::value<std::string>(), "manure scenario file")
        ("counties,c", po::value<std::string>(), "counties separated by _")
        ("reportloads,r", po::value<std::string>(), "report loads file")
    ;

    std::string emo_uuid = xg::newGuid().str();
    std::string exec_uuid = xg::newGuid().str();
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch(const po::error &ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        std::cerr << "For help, use the --help or -h option.\n";
        return 1;
    }

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }

    if (vm.count("land")) {
        std::cout << "Land scenario file was set to " << vm["land"].as<std::string>() << ".\n";
        land_filename = vm["land"].as<std::string>();
    } else {
        std::cout << "Land scenario file was not set.\n";
    }

    if (vm.count("animal")) {
        std::cout << "Animal scenario file was set to " << vm["animal"].as<std::string>() << ".\n";
        animal_filename = vm["animal"].as<std::string>();
    } else {
        std::cout << "Animal scenario file was not set.\n";
    }

    if (vm.count("manure")) {
        std::cout << "Manure scenario file was set to " << vm["manure"].as<std::string>() << ".\n";
        manure_filename = vm["manure"].as<std::string>();
    } else {
        std::cout << "Manure scenario file was not set.\n";
    }

    if (vm.count("reportloads")) {
        std::cout << "Reportloads file was set to " << vm["reportloads"].as<std::string>() << ".\n";
        reportloads_filename = vm["reportloads"].as<std::string>();
    } else {
        std::cout << "Report loads file was not set.\n";
    }
    if (vm.count("counties")) {
        std::cout << "Counties to process are:" << vm["counties"].as<std::string>() << ".\n";
        counties = vm["counties"].as<std::string>();
    } else {
        std::cout << "No counties submitted.\n";
    }
    fmt::print("{} {} {} {} {}\n", land_filename, animal_filename, manure_filename, reportloads_filename, counties);

    my_test(land_filename, animal_filename, manure_filename, reportloads_filename, counties, emo_uuid, exec_uuid);

    return 0;
}
