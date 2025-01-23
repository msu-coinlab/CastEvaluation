//
// Created by gtoscano on 4/26/23.
//

#ifndef BASE_SCENARIO_H
#define BASE_SCENARIO_H

#include "report_loads.h"
#include "amqp.h"

#include <string>

class BaseScenario {
public:
    BaseScenario(std::string input_data, std::string emo_data, std::string emo_uuid);
    BaseScenario();
    void load_input(std::string input_data, std::string emo_data, std::string emo_uuid);
    bool send();
    bool retrieve();
    bool wait_for_it();
private:
    std::string input_data_;
    ReportLoads loads_;
    RabbitMQClient rabbit;
    std::string emo_data_;
    std::string emo_uuid_;
};
#endif //BASE_SCENARIO_H
