//g++ report_loads.cpp -std=c++20 -lfmt -lpthread


#include "json.hpp"
#include "csv.hpp"

#include <algorithm>
#include <cctype>
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

using json = nlohmann::json;

ReportLoads::ReportLoads() {
    headings = {"ScenarioId", "LrsegId", "AgencyId", "LoadSourceId", "SectorId", "Amount", "UnitId", "NLoadEos", "PLoadEos", "SLoadEos", "NLoadEor", "PLoadEor", "SLoadEor", "NLoadEot", "PLoadEot", "SLoadEot"};
}
ReportLoads::ReportLoads(const ReportLoads& rhs) {
    headings = rhs.headings;
    parcels = rhs.parcels;
    n_eos_sum = rhs.n_eos_sum;
    p_eos_sum = rhs.p_eos_sum;
    s_eos_sum = rhs.s_eos_sum;
    n_eor_sum = rhs.n_eor_sum;
    p_eor_sum = rhs.p_eor_sum;
    s_eor_sum = rhs.s_eor_sum;
    n_eot_sum = rhs.n_eot_sum;
    p_eot_sum = rhs.p_eot_sum;
    s_eot_sum = rhs.s_eot_sum;
}

ReportLoads& ReportLoads::operator=(const ReportLoads& rhs) {
    if (this != &rhs) {
        headings = rhs.headings;
        parcels = rhs.parcels;
        n_eos_sum = rhs.n_eos_sum;
        p_eos_sum = rhs.p_eos_sum;
        s_eos_sum = rhs.s_eos_sum;
        n_eor_sum = rhs.n_eor_sum;
        p_eor_sum = rhs.p_eor_sum;
        s_eor_sum = rhs.s_eor_sum;
        n_eot_sum = rhs.n_eot_sum;
        p_eot_sum = rhs.p_eot_sum;
        s_eot_sum = rhs.s_eot_sum;
    }
    return *this;
}

void ReportLoads::print_loads() {
    fmt::print("Neos:{}, Peos:{}, Seos:{}, Neor:{}, Peor:{}, Seor:{}, Neot:{}, Peot:{}, Seot:{}\n", n_eos_sum, p_eos_sum, s_eos_sum, n_eor_sum, p_eor_sum, s_eor_sum, n_eot_sum, p_eot_sum, s_eot_sum);
}


void ReportLoads::load(const std::string& filename) {
    csv::CSVReader reader(filename);
    int amount_zero_counter  = 0;
    int amount_no_zero_counter  = 0;
    n_eos_sum = 0.0;p_eos_sum = 0.0;s_eos_sum = 0.0;
    n_eor_sum = 0.0;p_eor_sum = 0.0;s_eor_sum = 0.0;
    n_eot_sum = 0.0;p_eot_sum = 0.0;s_eot_sum = 0.0;

    for(auto& row: reader) {
        try {
            ReportLoadSt new_entry;
            new_entry.is_valid = true;
            new_entry.lrseg = row["LrsegId"].get<int>();
            new_entry.agency = row["AgencyId"].get<int>();
            new_entry.load_src = row["LoadSourceId"].get<int>();
            new_entry.sector = row["SectorId"].get<int>();
            new_entry.amount = -1.0;
            if (!row["Amount"].is_null() || row["Amount"].is_float()) {
                new_entry.amount = row["Amount"].get<double>();
            }
            else {
                new_entry.is_valid = false;
            }
            new_entry.unit = -1;
            if (!row["Amount"].is_null() || row["UnitId"].is_int()) {
                new_entry.unit = row["UnitId"].get<int>();
            }
            else {
                new_entry.is_valid = false;
            }
            new_entry.n_eos = row["NLoadEos"].get<double>();
            new_entry.p_eos = row["PLoadEos"].get<double>();
            new_entry.s_eos = row["SLoadEos"].get<double>();
            new_entry.n_eor = row["NLoadEor"].get<double>();
            new_entry.p_eor = row["PLoadEor"].get<double>();
            new_entry.s_eor = row["SLoadEor"].get<double>();
            new_entry.n_eot = row["NLoadEot"].get<double>();
            new_entry.p_eot = row["PLoadEot"].get<double>();
            new_entry.s_eot = row["SLoadEot"].get<double>();

            n_eos_sum += new_entry.n_eos;
            p_eos_sum += new_entry.p_eos;
            s_eos_sum += new_entry.s_eos;
            n_eor_sum += new_entry.n_eor;
            p_eor_sum += new_entry.p_eor;
            s_eor_sum += new_entry.s_eor;
            n_eot_sum += new_entry.n_eot;
            p_eot_sum += new_entry.p_eot;
            s_eot_sum += new_entry.s_eot;

            if (new_entry.amount == 0) ++amount_zero_counter;
            if (new_entry.amount != 0) ++amount_no_zero_counter;
            parcels.push_back(new_entry);
            if (new_entry.unit == 1) { //I only let acres to continue
                parcels_valid_.push_back(new_entry);
            }
            else {
                parcels_invalid_.push_back(new_entry);
            }
            parcels_all_.push_back(new_entry);
        } catch (std::exception& e) {
            // Catch any exceptions that derive from std::exception
            //std::cerr << "Caught exception: " << e.what() << std::endl;
        } catch (...) {
            // Catch any other exceptions
            std::cerr << "Caught unknown exception." << std::endl;
        }
    }
    //std::cout<<"Amount of rows = 0: " << amount_zero_counter<<std::endl;
    //std::cout<<"Amount of rows != 0: " << amount_no_zero_counter<<std::endl;
}

std::vector<double> ReportLoads::compute_loads(std::string which) {
    double n_eos_sum = 0.0;
    double p_eos_sum = 0.0;
    double s_eos_sum = 0.0;
    double n_eor_sum = 0.0;
    double p_eor_sum = 0.0;
    double s_eor_sum = 0.0;
    double n_eot_sum = 0.0;
    double p_eot_sum = 0.0;
    double s_eot_sum = 0.0;

    for (const auto &entry: parcels) {
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

void ReportLoads::print() {
    std::cout<<parcels.size()<<std::endl;
    for (const auto& parcel : parcels) {
        std::cout<<fmt::format("{},{},{},{},{}\n",parcel.lrseg, parcel.agency, parcel.load_src, parcel.amount, parcel.unit);
    }
}

std::vector<ReportLoadSt> ReportLoads::get_parcels() {
    return parcels;
}


std::vector<ReportLoadSt> ReportLoads::get_valid_parcels() {
    return parcels_valid_;
}

std::vector<ReportLoadSt> ReportLoads::get_all_parcels() {
    return parcels_all_;
}
std::vector<ReportLoadSt> ReportLoads::get_invalid_parcels() {
    return parcels_invalid_;
}

void ReportLoads::remove_invalid() {
    parcels.erase(std::remove_if(parcels.begin(), parcels.end(), [](const ReportLoadSt& rl) {
        return  rl.is_valid == false;
    }), parcels.end());
}

void ReportLoads::remove_load_srcs(const std::vector<int>& to_compare) {
    parcels.erase(std::remove_if(parcels.begin(), parcels.end(), [&to_compare](const ReportLoadSt& rl) {
        return std::ranges::find(to_compare, rl.load_src) != to_compare.end();
    }), parcels.end());
}

void ReportLoads::keep_only_load_srcs(const std::vector<int>& to_compare) {
    parcels.erase(std::remove_if(parcels.begin(), parcels.end(), [&to_compare](const ReportLoadSt& rl) {
        return std::ranges::find(to_compare, rl.load_src) == to_compare.end();
    }), parcels.end());
}
