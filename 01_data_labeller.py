"""
Label data according to the dataset description in the paper
"""
from collections import defaultdict
from unicodedata import name
import tqdm

from pathlib import Path
import pm4py
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.algo.filtering.log.attributes import attributes_filter

from utils import (
    create_log_reverse_index,
    check_existence_constraint,
    check_response_constraint,
    check_init_constraint,
    check_precedence_constraint,
    check_sequence_nonstrict_order,
    check_sequence_strict_order,
    check_trace_attribute,
    check_trace_event_attributes,
    check_trace_event_match_atleast_once,
    check_one_trace_attribute,
    check_sequence_nonstrict_order_interleaved,
    check_sequence_strict_order_interleaved,
)


# log paths
OUTPUT_FOLDER: str = "labelled_logs"
SEPSIS_LOG_INPUT_PATH: str = "logs/Sepsis Cases - Event Log.xes.gz"
TRAFFIC_LOG_INPUT_PATH: str = "logs/Road_Traffic_Fine_Management_Process.xes.gz"
BPI2011_LOG_INPUT_PATH: str = "logs/EnglishBPIChallenge2011.xes"
BPI2012_LOG_INPUT_PATH: str = "logs/financial_log.xes.gz"
#  One for all 5 logs
BPI2015_A_LOG_INPUT_PATH_TEMPLATE: str = "logs/BPIC15_{}.xes"


def load_log(input_path: str):
    print(f"Loading log from {input_path}")
    log = pm4py.read_xes(input_path)
    return log


def save_log(log, output_path: Path) -> None:
    print(f"Saving log to {output_path}")
    xes_exporter.apply(
        log, f"{str(output_path)}.xes.gz", variant=xes_exporter.Variants.ETREE
    )


def print_label_stats(name: str, results: dict, to_file=True) -> None:

    if to_file:
        with open("debug_log_label_results.txt", "a") as f:
            print(f"Log {name} breakdown on labelling:", file=f)
            for key, values in results.items():
                pos_label_count = sum(v == "1" for v in values)
                total_label_count = len(values)
                print(
                    f"Label type: {key} - pos label: {pos_label_count}, total: {total_label_count}. pct positive: {pos_label_count/total_label_count:.2%}",
                    file=f,
                )


def create_sepsis_labelled_logs(input_log_path, output_folder):
    """
    Creating sepsis logs.
    """
    log_name = "sepsis"  #  For printing and saving

    log = load_log(input_path=input_log_path)
    log_rev_index = create_log_reverse_index(log)

    # Add results
    labelling_results = defaultdict(list)

    print("Finding labels")
    for log_ind in tqdm.tqdm(range(len(log))):
        rev_trace = log_rev_index[log_ind]
        trace = log[log_ind]
        # decl
        decl_res = (
            check_response_constraint(rev_trace, "IV Antibiotics", "Leucocytes")
            + check_response_constraint(rev_trace, "LacticAcid", "IV Antibiotics")
            + check_response_constraint(rev_trace, "ER Triage", "CRP")
        )
        # mr_tr
        mr_tr_res = check_sequence_strict_order_interleaved(
            trace, ["Admission NC", "CRP", "Leucocytes"], count=1
        )
        # mra_tra
        mra_tra_res = check_sequence_nonstrict_order_interleaved(
            trace, ["IV Liquid", "LacticAcid", "Leucocytes"], count=2
        )
        # payload
        payload_res = check_trace_event_attributes(
            trace, attribute_value_dict={"DisfuncOrg": "True"}
        )

        # For declare, 6 if all nonvacuously satisfied. Each constraint returns 2, we have 2+2+2
        labelling_results["declare"].append("1" if decl_res == 6 else "0")
        labelling_results["mr_tr"].append("1" if mr_tr_res == 2 else "0")
        labelling_results["mra_tra"].append("1" if mra_tra_res == 2 else "0")
        labelling_results["payload"].append("1" if payload_res == 2 else "0")

    #  Print statistics
    print_label_stats(log_name, labelling_results)

    # Create logs
    for key in labelling_results.keys():
        for log_ind in range(len(log)):
            log[log_ind].attributes["Label"] = labelling_results[key][log_ind]

        save_log(log, Path(output_folder) / f"{log_name}_{key}")


def create_traffic_labelled_logs(input_log_path, output_folder):
    """
    Creating traffic logs.
    """
    log_name = "traffic"  #  For printing and saving

    log = load_log(input_path=input_log_path)
    log_rev_index = create_log_reverse_index(log)

    # Add results
    labelling_results = defaultdict(list)

    print("Finding labels")
    for log_ind in tqdm.tqdm(range(len(log))):
        rev_trace = log_rev_index[log_ind]
        trace = log[log_ind]
        decl_res = check_response_constraint(
            rev_trace, "Insert Date Appeal to Prefecture", "Add penalty"
        )
        mr_tr_res = check_sequence_strict_order_interleaved(
            trace, ["Add penalty", "Payment"], count=1
        )
        mra_tra_res = check_sequence_nonstrict_order_interleaved(
            trace, ["Create Fine", "Payment"], count=2
        )
        payload_art157_res = check_trace_event_attributes(trace, {"article": "157"})
        payload_pay36_res = check_trace_event_attributes(
            trace, {"paymentAmount": "36.0"}
        )

        labelling_results["declare"].append("1" if decl_res == 2 else "0")
        labelling_results["mr_tr"].append("1" if mr_tr_res == 2 else "0")
        labelling_results["mra_tra"].append("1" if mra_tra_res == 2 else "0")
        labelling_results["payload_art157"].append(
            "1" if payload_art157_res == 2 else "0"
        )
        labelling_results["payload_pay36"].append(
            "1" if payload_pay36_res == 2 else "0"
        )

    #  Print statistics
    print_label_stats(log_name, labelling_results)

    # Create logs
    for key in labelling_results.keys():
        for log_ind in range(len(log)):
            log[log_ind].attributes["Label"] = labelling_results[key][log_ind]

        save_log(log, Path(output_folder) / f"{log_name}_{key}")


def create_bpi2011_labelled_logs(input_log_path, output_folder):
    """
    Creating bpi2011 logs.
    """
    log_name = "bpi2011"  #  For printing and saving

    log = load_log(input_path=input_log_path)

    # Modify concept names
    #  "SGOT - Asat kinetic" to "SGOT"
    # "SGOT Asat kinetic - urgent" to "SGOT"

    # "SGPT - alat kinetic" to "SGPT"
    #  "SGPT alat kinetic - urgent" to "SGPT

    event_replacement_dict = {
        "SGOT - Asat kinetic": "SGOT",
        "SGOT Asat kinetic - urgent": "SGOT",
        "SGPT - alat kinetic": "SGPT",
        "SGPT alat kinetic - urgent": "SGPT",
    }

    for trace in log:
        for event in trace:
            if event["concept:name"] in event_replacement_dict:
                event["concept:name"] = event_replacement_dict[event["concept:name"]]

    log_rev_index = create_log_reverse_index(log)

    # Add results
    labelling_results = defaultdict(list)

    print("Finding labels")
    for log_ind in tqdm.tqdm(range(len(log))):
        rev_trace = log_rev_index[log_ind]
        trace = log[log_ind]
        decl_res = check_init_constraint(rev_trace, "outpatient follow-up consultation")
        mr_tr_res = check_sequence_strict_order_interleaved(
            trace,
            [
                "SGOT",
                "SGPT",
                "Milk acid dehydrogenase LDH kinetic",
                "leukocyte count electronic",
            ],
            count=1,
        )
        mra_tra_res = check_sequence_nonstrict_order_interleaved(
            trace,
            ["assumption laboratory", "Milk acid dehydrogenase LDH kinetic"],
            count=2,
        )
        payload_m13_res = check_one_trace_attribute(trace, "Diagnosis code", "M13")
        payload_t101_res = check_one_trace_attribute(trace, "Treatment code", "101")

        labelling_results["declare"].append("1" if decl_res == 2 else "0")
        labelling_results["mr_tr"].append("1" if mr_tr_res == 2 else "0")
        labelling_results["mra_tra"].append("1" if mra_tra_res == 2 else "0")
        labelling_results["payload_m13"].append("1" if payload_m13_res == 2 else "0")
        labelling_results["payload_t101"].append("1" if payload_t101_res == 2 else "0")

    #  Print statistics
    print_label_stats(log_name, labelling_results)

    # Create logs
    for key in labelling_results.keys():
        for log_ind in range(len(log)):
            log[log_ind].attributes["Label"] = labelling_results[key][log_ind]

        save_log(log, Path(output_folder) / f"{log_name}_{key}")


def create_bpi2012_labelled_logs(input_log_path, output_folder):
    """
    Creating financial logs
    """
    log_name = "bpi2012"  #  For printing and saving

    log = load_log(input_path=input_log_path)

    #  Remove non-complete states
    log = attributes_filter.apply_events(
        log,
        values=["COMPLETE"],
        parameters={
            attributes_filter.Parameters.ATTRIBUTE_KEY: "lifecycle:transition",
            attributes_filter.Parameters.POSITIVE: True,
        },
    )

    #  save filtered log for inspection
    save_log(log, Path(output_folder) / f"{log_name}_after_complete_filter")

    log_rev_index = create_log_reverse_index(log)

    # Add results
    labelling_results = defaultdict(list)

    print("Finding labels")
    for log_ind in tqdm.tqdm(range(len(log))):
        rev_trace = log_rev_index[log_ind]
        trace = log[log_ind]
        decl_res = check_precedence_constraint(rev_trace, "O_ACCEPTED", "A_APPROVED")
        mr_tr_res = check_sequence_strict_order_interleaved(
            trace,
            ["O_SENT", "W_Completeren aanvraag", "W_Nabellen incomplete dossiers"],
            count=1,
        )
        mra_tra_res = check_sequence_nonstrict_order_interleaved(
            trace, ["W_Afhandelen leads", "W_Completeren aanvraag"], count=3
        )
        payload_45000_res = check_trace_attribute(trace, {"AMOUNT_REQ": "45000"})
        payload_6500_res = check_trace_attribute(trace, {"AMOUNT_REQ": "6500"})

        labelling_results["declare"].append("1" if decl_res == 2 else "0")
        labelling_results["mr_tr"].append("1" if mr_tr_res == 2 else "0")
        labelling_results["mra_tra"].append("1" if mra_tra_res == 2 else "0")
        labelling_results["payload_45000"].append(
            "1" if payload_45000_res == 2 else "0"
        )
        labelling_results["payload_6500"].append("1" if payload_6500_res == 2 else "0")

    #  Print statistics
    print_label_stats(log_name, labelling_results)

    # Create logs
    for key in labelling_results.keys():
        for log_ind in range(len(log)):
            log[log_ind].attributes["Label"] = labelling_results[key][log_ind]

        save_log(log, Path(output_folder) / f"{log_name}_{key}")


def create_bpi2015_labelled_logs(input_log_path_template, output_folder):
    """
    Creating bpi2015 logs
    """

    monitoring_resources = {
        1: "560925",
        2: "4634935",
        3: "3442724",
        4: "560812",
        5: "560608",
    }

    for log_nr in [1, 2, 3, 4, 5]:
        input_log_path = input_log_path_template.format(log_nr)
        monitoring_resource = monitoring_resources[log_nr]
        log_name = f"bpi2015_{log_nr}"  #  For printing and saving

        log = load_log(input_path=input_log_path)
        log_rev_index = create_log_reverse_index(log)

        # Add results
        labelling_results = defaultdict(list)

        print("Finding labels")
        for log_ind in tqdm.tqdm(range(len(log))):
            rev_trace = log_rev_index[log_ind]
            trace = log[log_ind]
            decl_res = check_existence_constraint(
                rev_trace, "01_HOOFD_011"
            )  # TODO check if spaces here
            mr_tr_res = check_sequence_strict_order_interleaved(
                trace, ["08_AWB45_005", "01_HOOFD_200"], count=1
            )
            mra_tra_res = check_sequence_nonstrict_order_interleaved(
                trace, ["01_HOOFD_030_1", "01_HOOFD_510_1"], count=2
            )
            payload_monitoring_res = check_trace_event_attributes(
                trace, {"monitoringResource": monitoring_resource}
            )

            labelling_results["declare"].append("1" if decl_res == 2 else "0")
            labelling_results["mr_tr"].append("1" if mr_tr_res == 2 else "0")
            labelling_results["mra_tra"].append("1" if mra_tra_res == 2 else "0")
            labelling_results["payload_monitoring_res"].append(
                "1" if payload_monitoring_res == 2 else "0"
            )

        #  Print statistics
        print_label_stats(log_name, labelling_results)

        # Create logs
        for key in labelling_results.keys():
            for log_ind in range(len(log)):
                log[log_ind].attributes["Label"] = labelling_results[key][log_ind]

            save_log(log, Path(output_folder) / f"{log_name}_{key}")


def main():
    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
    create_sepsis_labelled_logs(SEPSIS_LOG_INPUT_PATH, OUTPUT_FOLDER)
    create_traffic_labelled_logs(TRAFFIC_LOG_INPUT_PATH, OUTPUT_FOLDER)
    create_bpi2011_labelled_logs(BPI2011_LOG_INPUT_PATH, OUTPUT_FOLDER)
    create_bpi2012_labelled_logs(BPI2012_LOG_INPUT_PATH, OUTPUT_FOLDER)
    create_bpi2015_labelled_logs(BPI2015_A_LOG_INPUT_PATH_TEMPLATE, OUTPUT_FOLDER)


if __name__ == "__main__":
    main()
