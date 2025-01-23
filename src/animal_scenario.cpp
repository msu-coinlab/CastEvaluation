//
// Created by Gregorio Toscano on 3/27/23.
//
//True Forest!
// g++ land_scenario.cpp -std=c++20 -lfmt -lpthread -larrow -lparquet
#include "animal_scenario.h"
#include "data_reader.h"

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/stream_writer.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/api.h>

#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <unordered_map>
#include <vector>
#include <memory>

#include <fmt/core.h>

bool has_value(const csv::CSVRow& row, const std::string& column) {
    try {
        row[column]; // We're just checking if the column exists, not doing anything with the value
        return true;
    } catch (const std::out_of_range&) {
        return false;
    }
}

AnimalScenario::AnimalScenario(const DataReader& data_reader) : data_(data_reader) {
}

int AnimalScenario::write (
        const std::vector<AnimalSol>& rows,
        const std::string& out_filename
) {


    parquet::schema::NodeVector fields;

    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpSubmittedId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "BmpId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "AgencyId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "StateUniqueIdentifier", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "StateId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "GeographyId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "AnimalGroupId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "LoadSourceGroupId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "UnitId", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "Amount", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "NReductionFraction", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "PReductionFraction", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "IsValid", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "ErrorMessage", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8
    ));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
            "RowIndex", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::ConvertedType::INT_32
    ));
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(out_filename));

    std::shared_ptr<parquet::schema::GroupNode> my_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::WriterProperties::Builder builder;
    //builder.compression(parquet::Compression::ZSTD);
    builder.version(parquet::ParquetVersion::PARQUET_1_0);
    parquet::StreamWriter os{
            parquet::ParquetFileWriter::Open(outfile, my_schema, builder.build())};


    int idx = 0;
    int counter = 0;
    for (auto const&row: rows) {
        double nreduction=0.0, preduction=0.0;
        os<<counter+1<<row.bmp<<row.agency<<fmt::format("SU{}",counter)<<row.state<<row.geo<<row.animal_grp<<row.load_src_grp<<row.unit<<row.amount<<nreduction<<preduction<<true<<""<<counter+1<<parquet::EndRow;
        counter++;
    }

    return counter;
}

int AnimalScenario::write2 (
        const std::vector<AnimalSol>& rows,
        const std::string& out_filename
) {
    /*
    using arrow::field;
    using arrow::schema;
    using arrow::Table;
    using arrow::Int32Builder;
    using arrow::FloatBuilder;
    using arrow::BooleanBuilder;
    using arrow::StringBuilder;
    using arrow::io::FileOutputStream;
    using parquet::arrow::WriteTable;
    using parquet::WriterProperties;
    std::shared_ptr<arrow::Schema> schema = arrow::schema ({
                   field("BmpSubmittedId", arrow::int32()),
                   field("BmpId", arrow::int32()),
                   field("AgencyId", arrow::int32()),
                   field("StateUniqueIdentifier", arrow::utf8()), //it can be binary
                   field("StateId", arrow::int32()),
                   field("GeographyId", arrow::int32()),
                   field("AnimalGroupId", arrow::int32()),
                   field("LoadSourceGroupId", arrow::int32()),
                   field("UnitId", arrow::int32()),
                   field("Amount", arrow::float64()),
                   field("NReductionFraction", arrow::float64(), true),//nullable=
                   field("PReductionFraction", arrow::float64(), true),//nullable=
                   field("IsValid", arrow::boolean()),
                   field("ErrorMessage", arrow::utf8()),//it can be binary
                   field("RowIndex", arrow::int32()),
   });

    Int32Builder bmp_submitted_id;
    Int32Builder bmp_id;
    Int32Builder agency_id;
    StringBuilder state_unique_identifier;
    Int32Builder state_id;
    Int32Builder geography_id;
    Int32Builder animal_grp_id;
    Int32Builder load_source_grp_id;
    Int32Builder unit_id_builder;
    DoubleBuilder amount_builder;
    DoubleBuilder nreduction_fraction_builder;
    DoubleBuilder preduction_fraction_builder;
    BooleanBuilder is_valid;
    StringBuilder error_message;
    Int32Builder row_index;




    int idx = 0;
    int counter = 0;
    for (auto const&row: rows) {
        bmp_submitted_id.Append(counter+1);
        bmp_id.Append(row.bmp);
        agency_id.Append(row.agency);
        state_unique_identifier.Append(fmt::format("SU{}",counter));
        state_id.Append(row.state);
        geography_id.Append(row.geo);
        animal_grp_id.Append(row.animal_grp);
        load_source_grp_id.Append(row.load_grp);
        unit_id_builder.Append(row.unit);
        amount_builder.Append(row.amount);
        nreduction_fraction_builder.AppendNull();
        preduction_fraction_builder.AppendNull();
        is_valid.Append(true);
        error_message.Append("");
        row_index.Append(counter+1);
        counter++;
    }

    std::shared_ptr<arrow::Array> bmp_submitted_array, bmp_array, agency_array, state_unique_identifier_array;
    std::shared_ptr<arrow::Array> state_array, geography_array, animal_grp_array;
    std::shared_ptr<arrow::Array> load_source_grp_array, unit_array, amount_array, nreduction_array, p_reduction_array;
    std::shared_ptr<arrow::Array> is_valid_array, error_message_array, row_index_array;

    bmp_submitted_id.Finish(&bmp_submitted_array);
    bmp_id.Finish(&bmp_array);
    agency_id.Finish(&agency_array);
    state_unique_identifier.Finish(&state_unique_identifier_array);
    state_id.Finish(&state_array);
    geography_id.Finish(&geography_array);
    animal_grp_id.Finish(&animal_grp_array);
    load_source_grp_id.Finish(&load_source_grp_array);
    unit_id_builder.Finish(&unit_array);
    amount_builder.Finish(&amount_array);
    nreduction_fraction_builder.Finish(&nreduction_array);
    preduction_fraction_builder.Finish(&preduction_array);
    is_valid.Finish(&is_valid_array);
    error_message.Finish(&error_message_array);
    row_index.Finish(&row_index_array);

    auto table = Table::Make(schema, {bmp_submitted_array, bmp_array, agency_array, state_unique_identifier_array,
                                      state_array, geography_array, animal_grp_array, load_source_array,
                                      unit_array, amount_array, nreduction_array, preduction_array,
                                      is_valid_array, error_message_array, row_index_array});

    std::shared_ptr<FileOutputStream> outfile;
    PARQUET_THROW_NOT_OK(FileOutputStream::Open(out_filename, &outfile));
    parquet::WriterProperties::Builder builder;
    builder.version(parquet::ParquetVersion::PARQUET_1_0);
    std::shared_ptr<parquet::WriterProperties> writer_props = builder.build();

    PARQUET_THROW_NOT_OK(WriteTable(*table, arrow::default_memory_pool(), outfile, table->num_rows(), writer_props));
     */
    return 0;
}

/**
 * @brief Loads a land scenario
 *
 * This function takes an string which is the key for a parcel, and a int, which is the bmp_id an string which is the key for a parcel, and a int, which is the bmp_id
 * It returns a tuple, the group_id and the position it is.
 *
 * @param[in] filename The name of the file to load
 *
 * @return a vector of land bmp solutions .
 *
 * @note This function does not perform any error checking or validation.
 */

void AnimalScenario::load_scenario(const std::string& filename)
{
    csv::CSVReader reader(filename);
    for(auto& row: reader) {
        try {
            AnimalSol new_entry;
            //BmpSubmittedId	BmpId	AgencyId	StateUniqueIdentifier
            // StateId	GeographyId	AnimalGroupId	LoadSourceGroupId
            // UnitId	Amount	NReductionFraction	PReductionFraction
            // IsValid	ErrorMessage	RowIndex
            new_entry.bmp = row["BmpId"].get<int>();
            new_entry.agency = row["AgencyId"].get<int>();
            new_entry.geo = row["GeographyId"].get<int>();
            new_entry.state= row["StateId"].get<int>();
            new_entry.animal_grp= row["AnimalGroupId"].get<int>();
            new_entry.load_src_grp = row["LoadSourceGroupId"].get<int>();
            new_entry.amount = row["Amount"].get<double>();
            new_entry.unit = row["UnitId"].get<int>();

            scenario_.push_back(new_entry);

        } catch (std::exception& e) {
            // Catch any exceptions that derive from std::exception
            //std::cerr << "Caught exception: " << e.what() << std::endl;
        } catch (...) {
            // Catch any other exceptions
            std::cerr << "Caught unknown exception." << std::endl;
        }
    }
}

double AnimalScenario::compute_cost(int cost_profile_id) {

    double total_sum = 0.0;
    for (const auto& row : scenario_) {
        auto cost_per_unit = data_.get_bmp_cost(cost_profile_id, row.bmp);
        if ( cost_per_unit != -1) {
            double cost = cost_per_unit * row.amount;
            total_sum += cost;
            fmt::print("BMP: {}, Acres: {}, Cost per unit: {}, Subtotal {}, Accum. Cost: {}\n", row.bmp, row.amount, cost_per_unit, cost, total_sum);
        }
        else {
            fmt::print("Error en information presentation\n");
        }
    }
    return total_sum;
}

double AnimalScenario::compute_cost(const std::string& loads_filename, int cost_profile_id) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(loads_filename, arrow::default_memory_pool()));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::ChunkedArray> amountArray;
    std::shared_ptr<arrow::ChunkedArray> bmpIdArray;

    // Get the schema from the Parquet file
    std::shared_ptr<arrow::Schema> schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));
    
    // Find the index of the "Amount" column
    int amount_index = schema->GetFieldIndex("Amount");
    if (amount_index == -1) {
        throw std::runtime_error("Amount column not found in Parquet file.");
    }
    
    // Find the index of the "BmpId" column
    int bmpId_index = schema->GetFieldIndex("BmpId");
    if (bmpId_index == -1) {
        throw std::runtime_error("BmpId column not found in Parquet file.");
    }
    
    // Read the "Amount" column by index
    PARQUET_THROW_NOT_OK(reader->ReadColumn(amount_index, &amountArray));
    
    // Read the "BmpId" column by index
    PARQUET_THROW_NOT_OK(reader->ReadColumn(bmpId_index, &bmpIdArray));
    //PARQUET_THROW_NOT_OK(reader->GetColumnByName("Amount", &amountArray));
    //PARQUET_THROW_NOT_OK(reader->GetColumnByName("BmpId", &bmpIdArray));

    if (amountArray->length() != bmpIdArray->length()) {
        throw std::runtime_error("Length mismatch between Amount and BmpId columns.");
    }

    double total_cost = 0.0;

    // Assuming the columns are split into chunks of equal size
    for (int i = 0; i < amountArray->num_chunks(); i++) {
        auto amountValues = std::static_pointer_cast<arrow::DoubleArray>(amountArray->chunk(i));
        auto bmpIdValues = std::static_pointer_cast<arrow::Int32Array>(bmpIdArray->chunk(i));

        for (int64_t j = 0; j < amountValues->length(); j++) {
            if (!amountValues->IsNull(j) && !bmpIdValues->IsNull(j)) {
                int bmpId = bmpIdValues->Value(j);
                double amount = amountValues->Value(j);

                auto cost_per_unit = data_.get_bmp_cost(cost_profile_id, bmpId);
                total_cost += cost_per_unit * amount;
            }
        }
    }

    return total_cost;
}

std::vector<AnimalSol> AnimalScenario::get_scenario() {
    return scenario_;
}
