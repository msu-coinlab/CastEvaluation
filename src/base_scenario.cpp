//
// Created by Gregorio Toscano on 4/26/23.
//
#include "base_scenario.h"
#include "misc_utilities.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>

#include <fmt/core.h>
#include <crossguid/guid.hpp>
#include <sw/redis++/redis++.h>
#include <SimpleAmqpClient/SimpleAmqpClient.h>



BaseScenario::BaseScenario(std::string input_data, std::string emo_data, std::string emo_uuid) {
    load_input(input_data, emo_data, emo_uuid);
}

BaseScenario::BaseScenario() {
    /*
    misc_utilities::get_env_var(, );
    std::string AMQP_HOST = misc_utilities::get_env_var("AMQP_HOST");
    std::string AMQP_USERNAME = misc_utilities::get_env_var("AMQP_USERNAME");
    std::string AMQP_PASSWORD = misc_utilities::get_env_var("AMQP_PASSWORD");
    std::string AMQP_PORT = misc_utilities::get_env_var("AMQP_PORT");
     */

}
void BaseScenario::load_input(std::string input_data, std::string emo_data, std::string emo_uuid) {
    input_data_ = input_data;
    emo_data_ = emo_data;
    emo_uuid_ = emo_uuid;
}

bool BaseScenario::send() {
    return true;
}

bool BaseScenario::retrieve() {

    return true;
}

bool BaseScenario::wait_for_it() {

    return true;
}
