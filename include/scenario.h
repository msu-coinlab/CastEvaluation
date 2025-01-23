//
// Created by gtoscano on 4/1/23.
//

#ifndef CBO_EVALUATION_SCENARIO_H
#define CBO_EVALUATION_SCENARIO_H

#include "base_scenario.h"
#include "bmp_grps.h"
#include "report_loads.h"


#include <string>
#include <unordered_map>
#include <vector>


struct Parcel : public ReportLoadSt{
    std::unordered_map<int,double> bmps;
};

class Scenario {
public:
    Scenario();

    Scenario(std::string ef_bmp_grp_filename, std::string lc_bmp_grp_filename);

    void load_solution(const std::string& filename);
    void load(const std::string& filename);

    void create_scenario(size_t scenario_id, const std::string& filename, const std::string& output_filename);
    void save(std::string filename);

    std::vector<double> compute_loads(const std::vector<ReportLoadSt>& my_parcel);
    void compute_land_conversion();
    void compute_ef_size();
    void compute_lc_size();
    int compute_lc_sizea();
    void compute_animal_size();
    void compute_ef_keys();
    void compute_lc_keys();

    double alpha_minus(const std::string& key);
    double alpha_plus_minus(const std::string& key);
    void sum_alpha(std::unordered_map<std::string, double>& am,  const std::string& key, double acreage);
    void normalize_land_conversion();
    void normalize_efficiency();
    void compute_eta();
    int compute_ef();
    void compute_lc();
    void compute_animal();
    void random_init_x();

private:

    size_t scenario_id_;
    int load_to_opt_;
    int ef_size_;
    int ef_begin_;
    int ef_end_;
    int lc_size_;
    int lc_begin_;
    int lc_end_;
    int animal_size;
    int animal_begin;
    double cost_ef;
    double load_ef;

    std::vector<std::string> ef_keys_;
    std::vector<std::string> lc_keys_;
    std::vector<double> x;
    std::string ef_bmp_grp_filename_;
    std::string lc_bmp_grp_filename_;
    bool lc_provided;
    BmpGrps ef_bmps_;
    BmpGrps lc_bmps_;
    BaseScenario base_scenario;
    std::unordered_map<std::string, std::vector<double>> eta_dict_;
    std::unordered_map<std::string, std::vector<double>> phi_dict_;

    std::unordered_map<std::string, std::vector<std::vector<double>>> ef_x_;
    //int: s, int: h, int: u, int: bmp, double: amount
    std::vector<std::tuple<int, int, int, int, double>> lc_x_;


    std::unordered_map<int, int> u_u_group_;
    std::unordered_map<int, int> s_geography_;
    std::unordered_map<int, int> s_state_;

    std::vector<std::string> parcel_list_;
    std::unordered_map<std::string, std::vector<std::vector<int>>> load_src_to_efficiency_;
    std::unordered_map<std::string, std::vector<std::vector<int>>> efficiency_;
    std::unordered_map<std::string, std::vector<std::string>> land_conversion_from_bmp_to;
    std::unordered_map<std::string, std::vector<std::vector<int>>> load_src_to_land_conversion_;
    std::unordered_map<std::string, std::vector<std::vector<int>>> land_conversion_;
    std::unordered_map<std::string, std::vector<std::string> > lc_to_parcel;
    std::unordered_map<std::string, int> parcels_;
    std::unordered_map<std::string, std::vector<std::vector<double>>> parcels_values_;
    std::unordered_map<std::string, double> amount_;
    std::unordered_map<std::string, double> amount_minus_;
    std::unordered_map<std::string, double> amount_plus_;
    std::unordered_map<std::string, double> animal_;
    std::unordered_map<std::string, std::vector<int>> animal_complete_;
    std::unordered_map<std::string, std::vector<int>> animal_grp_bmps_;///<
    std::unordered_map<std::string, std::vector<int>> discarded_bmps_;///<
    std::unordered_map<std::string, std::vector<std::tuple<int, double>>> fixed_bmps_; ///< The first variable is the key "lrseg_agency_loadsrc_bmp", send variable is the fixed value (the %value is computed dynamically).
    std::unordered_map<std::string, double> bmp_cost_;
    std::unordered_map<std::string, std::vector<int>> lrseg_;
    std::string scenario_data_;
    std::string scenario_data2_;
    std::unordered_map<std::string, int> counties_;
    //std::vector<int> counties_;

    std::vector<std::string> parcels_valid_keys_;
    std::vector<std::string> parcels_invalid_keys_;

    std::vector<ReportLoadSt> parcels_valid_;
    std::vector<ReportLoadSt> parcels_all_;
    std::vector<ReportLoadSt> parcels_invalid_;

    std::vector<double> sum_load_invalid_;
    std::vector<double> sum_load_valid_;
    std::vector<int> selected_bmps;
    std::unordered_map<int, std::tuple<int, int, std::string, std::string, std::string> > geography_county_;
    std::unordered_map<int, double> pct_by_valid_load_;
    std::vector<std::string> valid_lc_bmps_;
};
#endif //CBO_EVALUATION_SCENARIO_H
