//
// Created by gtoscano on 4/1/23.
//

#include "data_reader.h"
#include "base_scenario.h"
#include "land_scenario.h"
#include "misc_utilities.h"
#include "scenario.h"

#include "amqp.h"

#include <crossguid/guid.hpp>
#include <nlohmann/json.hpp>
#include <unistd.h>
#include <memory>
#include <random>
#include <boost/algorithm/string.hpp>
#include <sw/redis++/redis++.h>
#include <fmt/core.h>
#include <fmt/ostream.h>




using json = nlohmann::json;

namespace {
    std::string REDIS_HOST = misc_utilities::get_env_var("REDIS_HOST", "127.0.0.1");
    std::string REDIS_PORT = misc_utilities::get_env_var("REDIS_PORT", "6379");
    std::string REDIS_DB_OPT = misc_utilities::get_env_var("REDIS_DB_OPT", "1");
    std::string REDIS_URL = fmt::format("tcp://{}:{}/{}", REDIS_HOST, REDIS_PORT, REDIS_DB_OPT);

}


Scenario::Scenario() {
    std::ostringstream oss;
    ef_bmp_grp_filename_ = "/opt/opt4cast/csvs/bmp_grp.json";
    lc_bmp_grp_filename_ = "/opt/opt4cast/csvs/land_conversion_bmp_grp2.json";
    lc_provided = true;

    load_to_opt_ = 0;
    pid_t pid = getpid();
    oss << "logs/scenario_" << pid << ".txt";
    std::string log_file_name = oss.str();
}

Scenario::Scenario(std::string ef_bmp_grp_filename, std::string lc_bmp_grp_filename) {
    ef_bmp_grp_filename_ = ef_bmp_grp_filename;
    lc_bmp_grp_filename_ = lc_bmp_grp_filename;
    lc_provided = true;
}

void Scenario::load_solution(const std::string& filename) {

}

void Scenario::load(const std::string& filename) {

    lc_provided = false;
    // Open the JSON file
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(-1);
        return;
    }

    // Parse the JSON file directly into a nlohmann::json object
    json json_obj = json::parse(file);

    // Access the JSON data
    amount_ = json_obj["amount"].get<std::unordered_map<std::string, double>>();
    efficiency_ = json_obj["efficiency"].get<std::unordered_map<std::string, std::vector<std::vector<int>>>>();
    land_conversion_from_bmp_to = json_obj["land_conversion_to"].get<std::unordered_map<std::string, std::vector<std::string>>>();
    land_conversion_ = json_obj["land_conversion"].get<std::unordered_map<std::string, std::vector<std::vector<int>>> >();
    load_src_to_land_conversion_ = json_obj["lc_bmps"].get<std::unordered_map<std::string, std::vector<std::vector<int>>>>();
    load_src_to_efficiency_ = json_obj["ef_bmps"].get<std::unordered_map<std::string, std::vector<std::vector<int>>> >();
    parcel_list_ = json_obj["parcels"].get<std::vector<std::string>>();
    animal_ = json_obj["animal_unit"].get<std::unordered_map<std::string, double>>();
    animal_grp_bmps_ = json_obj["animal_bmps"].get<std::unordered_map<std::string, std::vector<int>>>();
    animal_complete_ = json_obj["animal_complete"].get<std::unordered_map<std::string, std::vector<int>>>();
    bmp_cost_ = json_obj["bmp_cost"].get<std::unordered_map<std::string, double>>();
    lrseg_ = json_obj["lrseg"].get<std::unordered_map<std::string, std::vector<int>>>();
    counties_ = json_obj["counties2"].get<std::unordered_map<std::string,int>>();

    scenario_data_ = json_obj["scenario_data"].dump();
    scenario_data2_ = json_obj["scenario_data_str"];
    phi_dict_ = json_obj["phi"].get<std::unordered_map<std::string, std::vector<double>>>();

    valid_lc_bmps_ = json_obj["valid_lc_bmps"].get<std::vector<std::string>>();

    std::unordered_map<std::string, int> u_u_group_str;
    u_u_group_str = json_obj["u_u_group"].get<std::unordered_map<std::string, int>>();

    for (const auto& [key, val] : u_u_group_str){
        u_u_group_[std::stoi(key)] = val;
    }
    auto geography_tmp = json_obj["s_geography"].get<std::unordered_map<std::string, int>>();

    for (const auto& [key, val] : geography_tmp){
        s_geography_[std::stoi(key)] = val;
    }

    auto state_tmp = json_obj["s_state"].get<std::unordered_map<std::string, int>>();
    for (const auto& [key, val] : state_tmp){
        s_state_[std::stoi(key)] = val;
    }

    auto geography_county_tmp = json_obj["counties"].get<std::unordered_map<std::string, std::tuple<int, int, std::string, std::string, std::string>>>();
    for (const auto& [key, val] : geography_county_tmp){
        geography_county_[std::stoi(key)] = val;
    }


    auto pct_by_valid_load_tmp = json_obj["pct_by_valid_load"].get<std::unordered_map<std::string,double>>();
    for (const auto& [key, val] : pct_by_valid_load_tmp){
        pct_by_valid_load_[std::stoi(key)] = val;
    }


    sum_load_valid_ = json_obj["sum_load_valid"].get<std::vector<double>>();
    sum_load_invalid_ = json_obj["sum_load_invalid"].get<std::vector<double>>();

    compute_ef_keys();
}
std::vector<double> Scenario::compute_loads(const std::vector<ReportLoadSt>& my_parcel ) {
    double n_eos_sum = 0.0;
    double p_eos_sum = 0.0;
    double s_eos_sum = 0.0;
    double n_eor_sum = 0.0;
    double p_eor_sum = 0.0;
    double s_eor_sum = 0.0;
    double n_eot_sum = 0.0;
    double p_eot_sum = 0.0;
    double s_eot_sum = 0.0;

    for (const auto &entry: my_parcel) {
        n_eos_sum += entry.n_eos;
        p_eos_sum += entry.p_eos;
        s_eos_sum += entry.s_eos;
        n_eor_sum += entry.n_eor;
        p_eor_sum += entry.p_eor;
        s_eor_sum += entry.s_eor;
        n_eot_sum += entry.n_eot;
        p_eot_sum += entry.p_eot;
        s_eot_sum += entry.s_eot;
    }
    std::vector<double> ret = {n_eos_sum, p_eos_sum, s_eos_sum,
                               n_eor_sum, p_eor_sum, s_eor_sum,
                               n_eot_sum, p_eot_sum, s_eot_sum};
    return ret;
}

void Scenario::create_scenario(size_t scenario_id, const std::string& filename, const std::string& output_filename) { 

    DataReader data_reader;
    data_reader.read_all();
    scenario_id_ = scenario_id;

    scenario_data_ = data_reader.get_scenario_data(scenario_id);
    scenario_data2_ = data_reader.get_scenario_data2(scenario_id);
    std::vector<std::string> scenario_data_vec;
    boost::split(scenario_data_vec, scenario_data2_, boost::is_any_of("_"));

    //int base_condition = 256;
    int base_condition = std::stoi(scenario_data_vec[3]);
    //std::cout<<"Base condition: "<<base_condition<<std::endl;


    for(const auto& [key,lst] : data_reader.get_animal_grp_bmps()) {
        animal_grp_bmps_[std::to_string(key)] = lst;
    }

    ef_bmps_.load_file(ef_bmp_grp_filename_);

    if(lc_provided) {
        lc_bmps_.load_file(lc_bmp_grp_filename_);
    }

    /*
     * auto filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Berkeley/base/reportloads_wWKTZpn.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/control-all-vars/Grant/base/reportloads_8rVciy4.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/95_math_5_cc_20_eps-finished/Hampshire/base/reportloads_mkmbUx8.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/95_math_5_cc_20_eps-finished/Monroe/base/reportloads_7zEPya8.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/95_math_5_cc_20_eps-finished/Morgan/base/reportloads_2OAdVoK.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/95_math_5_cc_20_eps-finished/Pendleton/base/reportloads_xmbhyPq.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/control-all-vars/Preston/base/reportloads_SVFhKZh.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/95_math_5_cc_20_eps-finished/Tucker/base/reportloads_mVd56Ce.csv";
    filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Jefferson/base/reportloads_kjVfBNe.csv";
    filename = "reportloads_Q6tvVIl.csv";
    //filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Mineral/base/reportloads_fq4qmYb.csv";
    //filename = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-1-5/Hardy/base/reportloads_jkKeDOl.csv";
    */
    ReportLoads base_scenario;
    base_scenario.load(filename);
    base_scenario.remove_invalid();
    //selected_bmps = {7,14,17,22,61,62};
    //selected_bmps = {8,14,17,22,61,62};
    //selected_bmps = {7, 15, 62};
    //selected_bmps = {22};

    for(const auto& value : lc_bmps_.get_load_srcs()) {
        auto str_load_src = fmt::format("{}", value);
        load_src_to_land_conversion_[str_load_src] = lc_bmps_.get(value);
    }

    for(const auto& value : ef_bmps_.get_load_srcs()) {
        auto str_load_src = fmt::format("{}", value);
        load_src_to_efficiency_[str_load_src] = ef_bmps_.get(value);
    }

    u_u_group_ = data_reader.get_u_u_groups();
    s_geography_ = data_reader.get_geographies();
    s_state_ = data_reader.get_states();
    geography_county_ = data_reader.get_geography_county();

    auto lc_bmp_from_to = data_reader.get_lc_bmp_from_to();
    std::vector<int> load_src_list;

    auto parcels_definitions = base_scenario.get_parcels();
    parcels_valid_ = base_scenario.get_valid_parcels();
    parcels_invalid_ = base_scenario.get_invalid_parcels();
    parcels_all_ = base_scenario.get_all_parcels();

    std::vector<int> lrseg_lst;
    //for(const auto& value : parcels_definitions ) {
    //
    std::unordered_map<int, double> accum_by_valid_load_tmp;

    std::vector<std::string> parcel_keys;
    for(const auto& value : parcels_valid_) {
        auto key = fmt::format("{}_{}_{}", value.lrseg, value.agency, value.load_src);
        if( value.agency != 9) continue;
        if (ef_bmps_.valid_load_src(value.load_src) && value.amount > 1) {
            parcel_keys.push_back(key);
            accum_by_valid_load_tmp[value.load_src] += value.amount;
        }
    }

    double total_sum_load = 0.0;
    for (const auto& [key, value] : accum_by_valid_load_tmp) {
        total_sum_load += value;
    }
    for (const auto& [key, value] : accum_by_valid_load_tmp) {
        pct_by_valid_load_[key] = (value*100.0) / total_sum_load;
    }

    for(const auto& value : parcels_valid_) {
        auto key = fmt::format("{}_{}_{}", value.lrseg, value.agency, value.load_src);

        //GTP: Hay que checar bien esto
        if( value.agency != 9) continue;


        if(std::ranges::find(lrseg_lst, value.lrseg) == lrseg_lst.end()) {
            lrseg_lst.push_back(value.lrseg);
        }

        if (std::ranges::find(load_src_list, value.load_src)  == load_src_list.end()) {
            load_src_list.push_back(value.load_src);
        }
        if (ef_bmps_.valid_load_src(value.load_src) && value.amount > 1) {
            parcel_list_.push_back(key);
            parcels_valid_keys_.push_back(key);
            parcels_[key] = value.load_src;
            efficiency_[key] = ef_bmps_.get(value.load_src);
            land_conversion_[key] = lc_bmps_.get(value.load_src);
            amount_[key] = value.amount;
            auto lc_bmp_to_list = data_reader.get_load_src_to_bmp_list(value.load_src);
            std::vector<std::string> lc_bmp_to_tmp;
            for(const auto& ef_bmp: lc_bmp_to_list) {
                if((selected_bmps.empty()) || (std::ranges::find(selected_bmps, ef_bmp) != selected_bmps.end())) {
                    auto bmp_from = fmt::format("{}_{}", ef_bmp, value.load_src);

                    auto tmp_to = data_reader.get_lc_bmp_from_to(bmp_from);
                    auto bmp_to = fmt::format("{}_{}", ef_bmp, tmp_to);

                    auto key_to_load_src = fmt::format("{}_{}_{}", value.lrseg, value.agency, tmp_to);
                    if (value.load_src != tmp_to ){//&& std::ranges::find(parcel_keys, key_to_load_src) == parcel_keys.end()) {
                        lc_bmp_to_tmp.push_back(bmp_to);
                    }
                }
            }
            if (lc_bmp_to_tmp.size() > 0) {
                land_conversion_from_bmp_to[key] = lc_bmp_to_tmp;
            }
        }
    }

    for(const auto& data : data_reader.get_lrseg() ) {
        if(std::ranges::find(lrseg_lst, data[0]) != lrseg_lst.end()) {
            //data[0] = lrseg, data[1] = fips, data[2] = state_id, data[3] = county
            lrseg_[std::to_string(data[0])] = {data[1], data[2], data[3], s_geography_[data[0]]};
            counties_[std::to_string(data[3])] = data[2];

            /*
            if(std::ranges::find(counties_, data[3]) == counties_.end()) {
                counties_.push_back(data[3]);
            }
            */

        }
    }

    std::string counties_str="";
    int ncounties = 0;
    for (const auto& [county, value]: counties_ ) {

        std::tuple<int, int, std::string, std::string, std::string> tmp = geography_county_.at(std::stoi(county));
        auto county_lrseg = std::get<0>(tmp);
        ncounties++;
        if (counties_str != "") {
            counties_str =fmt::format("{}_{}", counties_str, county_lrseg);
        }
        else {
            counties_str = fmt::format("{}", county_lrseg);
        }

        auto key_animal = fmt::format("{}_{}", base_condition, county);
        //check if key_animal is in data_reader.get_animal_idx
        fmt::print("Key animal (base_condition, county): {}\n", key_animal);
        if (data_reader.is_animal_in_idx(key_animal)) {
            for (const auto &key: data_reader.get_animal_idx(key_animal)) {
                animal_[key]=data_reader.get_animal(key);
                std::vector<std::string> out;
                misc_utilities::split_str(key, '_', out);
                auto load_src = out[3];
                animal_complete_[key] = animal_grp_bmps_[load_src];
            }
        }
        else {
            fmt::print("Key animal not found (base_condition, county): {}\n", key_animal);
        }
    }
    
    

    /***/
    for(const auto& value : parcels_all_) {
        auto key = fmt::format("{}_{}_{}", value.lrseg, value.agency, value.load_src);
        if (value.amount > 0.0) {
            /*
            phi_dict_[key] = {0.0, 0.0, 0.0,
                              0.0, 0.0, 0.0,
                              0.0, 0.0, 0.0};
                              */
            phi_dict_[key] = {value.n_eos / value.amount, value.p_eos / value.amount, value.s_eos / value.amount,
                              value.n_eor / value.amount, value.p_eor / value.amount, value.s_eor / value.amount,
                              value.n_eot / value.amount, value.p_eot / value.amount, value.s_eot / value.amount};
        }
        if(std::ranges::find(parcels_valid_keys_, key) == parcels_valid_keys_.end()) {
            parcels_invalid_keys_.push_back(key);
        }
    }
    sum_load_invalid_ = compute_loads(parcels_invalid_);
    sum_load_valid_ = compute_loads(parcels_valid_);

    /****/
    bmp_cost_ = data_reader.get_bmp_cost();

    random_init_x();

    normalize_efficiency();
    compute_ef();
    compute_lc_size();
    normalize_land_conversion();
    compute_lc();
    size_t pos = scenario_data2_.find('N');
    if (pos != std::string::npos) {
        scenario_data2_.replace(pos, 1, std::to_string(ncounties));
    }
    scenario_data2_ = fmt::format("{}_{}", scenario_data2_, counties_str);
    json scenario_data = json::parse(scenario_data_);
    scenario_data["counties"] = counties_str;
    scenario_data["n_counties"] = ncounties;
    scenario_data_ = scenario_data.dump();

    save(output_filename);

    /*
    auto scenario_path = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-4-10/Berkeley/executions/2/results/";
    scenario_path = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-4-10/Jefferson/executions/2/results/";
    //scenario_path = "/home/gtoscano/django/api4opt4-tests/innovization-strategy-4-10/Hardy/executions/2/results/";
    LandScenario cf(data_reader);

    auto exec_id = "0";
    auto scen_filename = fmt::format("{}/{}_output_t.csv", scenario_path, exec_id);
    cf.load_land_scenario(scen_filename);
    auto scen = cf.get_land_scenario();
    auto par_filename = fmt::format("{}/{}_output_t_gtp.parquet", scenario_path, exec_id);
    cf.write_land(scen, par_filename);

    auto cost_profile_id = 8;
    auto total = cf.compute_cost(cost_profile_id);
    fmt::print("Total Cost: {}\n", total);
    compute_ef_keys();

    std::sort(parcel_list_.begin(), parcel_list_.end());
    std::sort(parcel_keys.begin(), parcel_keys.end());

    if (parcel_list_.size() != parcel_keys.size()) {
        std::cout<<"They are different\n";
    }
    for (size_t i = 0; i < parcel_keys.size(); ++i) {
        if (parcel_list_[i] != parcel_keys[i]) {
            std::cout<<"They are different\n";
        }
    }

    int tmp_counter_parcels=0;
    int tmp_counter_lc=0;
    for(auto [key, value] : land_conversion_from_bmp_to) {
        ++tmp_counter_parcels;
        tmp_counter_lc += value.size();
    }
    std::cout<<"parcels that have LC: "<< tmp_counter_parcels<<std::endl;
    std::cout<<"LC "<<tmp_counter_lc<<std::endl;
    */
}

void Scenario::save(std::string filename) {
    json json_obj;// = json::object();
    json_obj["amount"] = amount_;
    json_obj["efficiency"] = efficiency_;
    json_obj["land_conversion_to"] = land_conversion_from_bmp_to;
    json_obj["land_conversion"] = land_conversion_;
    json_obj["lc_bmps"] = load_src_to_land_conversion_;
    json_obj["ef_bmps"] = load_src_to_efficiency_;
    json_obj["parcels"] = parcel_list_;
    json_obj["animal_unit"] = animal_;
    json_obj["animal_bmps"] = animal_grp_bmps_;
    json_obj["animal_complete"] = animal_complete_;
    json_obj["bmp_cost"] = bmp_cost_;
    json_obj["lrseg"] = lrseg_;
    json_obj["counties2"] = counties_;
    json_obj["scenario_data"] = json::parse(scenario_data_);
    json_obj["scenario_data_str"] = scenario_data2_;
    json_obj["phi"] = phi_dict_;
    std::vector<std::string> valid_lc_bmps_str = {"12"};
    valid_lc_bmps_ = valid_lc_bmps_str;
    json_obj["valid_lc_bmps"] = valid_lc_bmps_str; 
    json_obj["scenario_id"] = scenario_id_;

    std::unordered_map<std::string, int> u_u_group_str;
    for (const auto& [key, val] : u_u_group_){
        u_u_group_str[std::to_string(key)] = val;
    }
    json_obj["u_u_group"] = u_u_group_str;
    // geography
    json geography_str;
    for (const auto& [key, val] : s_geography_){
        geography_str[std::to_string(key)] = val;
    }
    json_obj["s_geography"] = geography_str;
    // state
    json state_str;
    for (const auto& [key, val] : s_state_){
        state_str[std::to_string(key)] = val;
    }

    json geography_county_json;
    for (const auto& [key, val] : geography_county_){
        geography_county_json[std::to_string(key)] = val;
    }

    json pct_by_valid_load_json;
    for (const auto& [key, val] : pct_by_valid_load_){
        pct_by_valid_load_json[std::to_string(key)] = val;
    }


    json_obj["s_state"] = state_str;
    json_obj["sum_load_invalid"] = sum_load_invalid_;
    json_obj["sum_load_valid"] = sum_load_valid_;
    json_obj["counties"] = geography_county_json; 
    json_obj["pct_by_valid_load"] = pct_by_valid_load_json;

    //nlohmann::to_json(json_obj["land_conversion"], valid_lc_from_parcel);
    std::ofstream file(filename);
    file<<json_obj.dump();
    file.close();
}

void Scenario::compute_land_conversion() {

}

void Scenario::compute_ef_size() {

    int counter = 0;
    for (const auto &[key, bmp_groups]: efficiency_) {
        for (const auto &bmp_group : bmp_groups) {
            ++counter;
            for (const auto &bmp : bmp_group) {
                ++counter;
            }
        }
    }

    ef_size_ = counter;
    ef_begin_ = 0;
    ef_end_ = counter;
}

int Scenario::compute_lc_sizea() {
    int counter = 0;
    for (const auto &[key, bmp_group]: land_conversion_from_bmp_to) {
        ++counter;
        for (const auto &bmp: bmp_group) {
            ++counter;
        }
    }
    return counter;
}

void Scenario::compute_lc_size() {
    auto counter = compute_lc_sizea();
    lc_size_ = counter;
    lc_begin_ = ef_end_;
    lc_end_ = lc_begin_ + counter;
}

void Scenario::compute_animal_size() {

}

void Scenario::compute_lc_keys() {

    for (const auto& pair : land_conversion_from_bmp_to) {
        lc_keys_.push_back(pair.first);
    }

    // Sort the vector of keys
    std::sort(lc_keys_.begin(), lc_keys_.end());
}

void Scenario::compute_ef_keys() {

    for (const auto& pair : efficiency_) {
        ef_keys_.push_back(pair.first);
    }

    // Sort the vector of keys
    std::sort(ef_keys_.begin(), ef_keys_.end());
}

double Scenario::alpha_minus(const std::string& key) {
    double minus = 0.0;
    double original = 0.0;

    if (amount_.contains(key)) {
        original = amount_.at(key);
    }

    if (amount_minus_.contains(key)) {
        minus = amount_minus_.at(key);
    }

    return original - minus;
}

double Scenario::alpha_plus_minus(const std::string& key) {

    double plus = 0.0;

    if (amount_plus_.contains(key)) {
        plus = amount_plus_.at(key);
    }

    return alpha_minus(key) + plus;
}

void Scenario::sum_alpha(std::unordered_map<std::string, double>& am,  const std::string& key, double acreage) {
    double tmp = 0.0;
    if (am.contains(key)) {
        tmp = am.at(key);
    }
    tmp += acreage;
    am[key] = tmp;
}
void Scenario::random_init_x() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dis(0.0, 1.0);

    std::vector<double> randomNumbers;

    compute_lc_size();
    compute_ef_size();
    //std::cout<<"Efficiency bmps: "<<ef_size_<<"\n";
    //std::cout<<"Land Conversion bmps: "<<lc_size_<<"\n";

    int size = ef_size_ + lc_size_;

    // Generate random doubles and add them to the vector
    for (int i = 0; i < size; ++i) {
        double randomNumber = dis(gen);
        x.push_back(randomNumber);
    }
}
void Scenario::normalize_land_conversion() {
    int counter = lc_begin_;
    amount_minus_.clear();
    amount_plus_.clear();
    lc_x_.clear();
    //std::vector<std::string> lc_altered_keys;

    for (const auto &[key, bmp_group]: land_conversion_from_bmp_to) {
        std::vector<std::pair<double, std::string>> grp_tmp;
        std::vector <std::string> key_split;
        double sum = x[counter];
        misc_utilities::split_str(key, '_', key_split);
        ++counter;

        for (const auto& bmp : bmp_group) {
            grp_tmp.push_back({x[counter], bmp});
            sum += x[counter];
            ++counter;
        }

        double pct_accum = 0.0;
        std::vector<std::pair<double, std::string>> grp_pct_tmp;
        for (auto &line : grp_tmp) {
            double pct = line.first;
            std::string to = line.second;
            auto norm_pct =  pct / sum;
            std::vector <std::string> out_to;
            misc_utilities::split_str(to, '_', out_to);
            auto key_to = fmt::format("{}_{}_{}", key_split[0], key_split[1], out_to[1]);
            sum_alpha(amount_plus_, key_to, norm_pct * amount_[key]);
            pct_accum += norm_pct;
            if (norm_pct * amount_[key] > 1.0) {
                lc_x_.push_back({std::stoi(key_split[0]), std::stoi(key_split[1]), std::stoi(key_split[2]), std::stoi(out_to[0]), norm_pct * amount_[key]});
            }
        }
        sum_alpha(amount_minus_, key, pct_accum * amount_[key]);
    }


}

void Scenario::normalize_efficiency() {
    int counter = 0;
    for (const auto& [key, bmp_groups] : efficiency_) {
        std::vector<std::vector<double>> grps_tmp;

        for (const auto& bmp_group : bmp_groups) {
            std::vector<double> grp_tmp;
            //The representation has a slack variable to represent the unused space
            double sum = x[counter];
            ++counter;

            for (const auto& bmp : bmp_group) {
                grp_tmp.push_back(x[counter]);
                sum += x[counter];
                ++counter;
            }

            for (auto &bmp: grp_tmp) {
                bmp = bmp / sum;
            }
            grps_tmp.push_back(grp_tmp);
        }
        ef_x_[key] = grps_tmp;
    }
}

void Scenario::compute_eta() {
    auto redis = sw::redis::Redis(REDIS_URL);

    for (const auto &[key, bmp_groups]: efficiency_) {
        std::vector <std::string> out;
        misc_utilities::split_str(key, '_', out);
        auto s = out[0];
        auto u = out[2];
        for (const auto &bmp_group: bmp_groups) {
            for (const auto &bmp: bmp_group) {
                std::string s_tmp = fmt::format("{}_{}_{}", bmp, s, u);
                std::vector <std::string> eta_tmp;
                redis.lrange(s_tmp, 0, -1, std::back_inserter(eta_tmp));

                if (!eta_tmp.empty()) {
                    eta_dict_[s_tmp] = {stof(eta_tmp[0]), stof(eta_tmp[1]), stof(eta_tmp[2])};
                } else {
                    eta_dict_[s_tmp] = {0.0, 0.0, 0.0};
                    //std::cout << "ETA not found"<<key<<"\n";
                }
            }
        }
    }
}

/*
if (amount != "" && amount > 0.0 &&
    has_geography == true && has_u_group == true &&
    has_bmps == true && is_a_valid_u == true)
    */

int Scenario::compute_ef() {
    double total_cost = 0;

    std::vector<double> fx(2, 0.0);
    std::vector<double> g(2, 0.0);
    int nconstraints = 2;

    compute_eta();

    std::vector<double> pt_load(3, 0.0);
    for (const auto &[key, bmp_groups]: efficiency_) {
        std::vector<double> prod(3, 1);
        std::vector <std::string> out;
        misc_utilities::split_str(key, '_', out);
        auto s = out[0];
        auto u = out[2];
        auto state_id = lrseg_.at(s)[1];
        auto alpha = amount_[key];
        auto bmp_group_idx = 0;
        for (const auto &bmp_group: bmp_groups) {
            std::vector<double> sigma(3, 0.0);
            auto bmp_idx = 0;
            auto sigma_cnstr = 0.0;

            for (const auto &bmp: bmp_group) {
                auto pct = ef_x_[key][bmp_group_idx][bmp_idx];
                sigma_cnstr += pct;
                double amount = pct * alpha;
                auto bmp_cost_idx = fmt::format("{}_{}", state_id, bmp);
                double cost = amount * bmp_cost_[bmp_cost_idx];

                std::string s_tmp = fmt::format("{}_{}_{}", bmp, s, u);
                auto eta = eta_dict_[s_tmp];
                sigma[0] += eta[0] * pct;
                sigma[1] += eta[1] * pct;
                sigma[2] += eta[2] * pct;
                sigma_cnstr += pct;
                ++bmp_idx;
            }
            prod[0] *= (1.0 - sigma[0]);
            prod[1] *= (1.0 - sigma[1]);
            prod[2] *= (1.0 - sigma[2]);
            //g[nconstraints] = (1.0 - sigma_cnstr);

            ++bmp_group_idx;
            ++nconstraints;
        }
        if(!phi_dict_.contains(key)) {
            std::cout<<"nooooooo.... \n"<<key<<"\n";
        }
        auto phi = phi_dict_[key];

        pt_load[0] += phi[0] * alpha * prod[0];
        pt_load[1] += phi[1] * alpha * prod[1];
        pt_load[2] += phi[2] * alpha * prod[2];

    }

    //g[0] =  (sum_load_invalid[0] + pt_load[0]) - 0.8*(sum_load_invalid[0] + sum_load_valid[0]) ;
    double load_ub = sum_load_invalid_[load_to_opt_] + sum_load_valid_[load_to_opt_];
    double load_obtained = sum_load_invalid_[load_to_opt_] + pt_load[load_to_opt_];

    double load_lb = 0.8 * load_ub; //20% reduction
    //g[0] = (load_obtained < load_lb)?(load_obtained-load_lb)/load_ub:0;

    double cost_ub = 1000000.0;
    double cost_steps = 1000000.0;
    g[1] = (total_cost > cost_ub)? (cost_ub-total_cost)/cost_steps:0;

    fx[0] = total_cost;
    fx[1] = sum_load_invalid_[load_to_opt_] + pt_load[load_to_opt_];
    //fmt::print("Total Cost: {}. Total Load: {}.\n", fx[0], fx[1]);

    //write_files_csv2 (selected_bmps, emo_uuid, exec_uuid);
    //write_files_animal_csv2 (selected_bmps, emo_uuid, exec_uuid);

    //int nrows = write_and_send_parquet_file2(selected_bmps, emo_uuid, exec_uuid);
    return 0;
}

void Scenario::compute_lc() {
    for (const auto& entry : lc_x_) {
        auto [s, h, u, bmp, amount] = entry;
        //std::cout<<fmt::format("S: {}, h: {}, u: {}, bmp: {}, amount: {}\n", s, h, u, bmp, amount);
    }

}

void Scenario::compute_animal() {

}
