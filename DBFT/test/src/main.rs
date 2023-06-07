use assertables::assume;
use clap::Parser;
use log::LevelFilter;
use std::env;
use serde_json::Value;

use dslib::pynode::{ JsonMessage, PyNodeFactory };
use dslib::test::{ TestResult, TestSuite };
use dslib::system::System;

#[path = "../../../utils/utils.rs"]
mod utils;


pub fn check_decided_proposals(
    sys: &mut System<JsonMessage>,
    nodes: &Vec<String>,
    mut expected_result: String
) -> TestResult {
    for node in nodes.iter() {
        let mut messages = utils::get_local_messages(&sys, &node);

        if messages.len() == 0 {
            let res = sys.step_until_local_message(&node);
            assume!(res.is_ok(), format!("Node {}: No messages returned!", node))?;
            messages = utils::get_local_messages(&sys, &node);
        }

        assume!(messages.len() == 1, format!("Node {}: Wrong number of messages!", node))?;
        assume!(messages[0].tip == "RESULT", format!("Node {}: Wrong message type!", node))?;

        let data: Value = serde_json::from_str(&messages[0].data).unwrap();
        let proposals = data["valid_proposals"].as_str().unwrap().to_string();
        if expected_result == "" {
            expected_result = proposals;
        } else {
            assume!(
                proposals == expected_result,
                format!("Node {}: returned proposals {} instead of {}", node, proposals, expected_result)
            )?;
        }
    }
    Ok(true)
}

// TESTS -----------------------------------------------------------------------

fn test_all_same(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let value = 42;
    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), value);

    utils::send_init_messages(&mut sys, &init_values);

    if config.check_termination {
        sys.step_until_no_events();
    }

    utils::check_consensus(&mut sys, &nodes, Some(value))
}

fn test_all_diff(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    for _ in nodes.iter() {
        init_values.push(sys.gen_range(10..100));
    }

    utils::send_init_messages(&mut sys, &init_values);

    if config.check_termination {
        sys.step_until_no_events();
    }

    utils::check_consensus(&mut sys, &nodes, None)
}

fn test_proposals(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    for _ in nodes.iter() {
        init_values.push(sys.gen_range(10..100));
    }

    utils::send_init_messages(&mut sys, &init_values);

    if config.check_termination {
        sys.step_until_no_events();
    }

    let mut expected_proposals = Vec::new();
    for value in init_values.iter() {
        expected_proposals.push(value.to_string());
    }
    expected_proposals.sort();

    check_decided_proposals(&mut sys, &nodes, expected_proposals.join(";"))
}

fn test_faulty(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    for _ in nodes.iter() {
        init_values.push(sys.gen_range(10..100));
    }

    utils::send_init_messages(&mut sys, &init_values);
    sys.step();

    let mut correct_nodes = Vec::<String>::new();
    let mut disconnected_nodes = Vec::<String>::new();
    let mut expected_proposals = Vec::new();

    for i in 0..config.faulty_count {
        let node = format!("{}", i);
        sys.crash_node(&node);
        disconnected_nodes.push(node);
    }
    for i in config.faulty_count..config.node_count {
        let node = format!("{}", i);
        correct_nodes.push(node);
        expected_proposals.push(init_values[i as usize].to_string());
    }
    expected_proposals.sort();

    if config.check_termination {
        sys.step_until_no_events();
    }

    check_decided_proposals(&mut sys, &correct_nodes, expected_proposals.join(";"))
}

// MAIN ------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to dslib directory
    #[clap(long = "lib", short = 'l', default_value = "../../dslib")]
    dslib_path: String,

    /// Path to Python file with nodes implementations
    #[clap(long = "impl", short = 'i', default_value = "../main.py")]
    impl_path: String,

    /// Random seed used in tests
    #[clap(long, short, default_value = "42")]
    seed: u64,

    /// Nodes count
    #[clap(long = "nodes", short = 'n', default_value = "4")]
    node_count: u32,

    /// Number of faulty nodes
    #[clap(long = "faulty_count", short = 'f', default_value = "1")]
    faulty_count: u32,

    /// Test to run (optional)
    #[clap(long, short)]
    test: Option<String>,
}

fn main() {
    utils::init_logger(LevelFilter::Trace);
    env::set_var("PYTHONPATH", "../../dslib/python");
    let args = Args::parse();

    let node_factory = PyNodeFactory::new(&args.impl_path, "DBFT");
    let mut config = utils::TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        byz_node_factory: None,
        seed: args.seed,
        check_termination: false,
    };

    let mut tests = TestSuite::new();
    tests.add("TEST ALL SAME", test_all_same, config);
    tests.add("TEST ALL DIFF", test_all_diff, config);
    tests.add("TEST PROPOSALS", test_proposals, config);
    tests.add("TEST FAULTY", test_faulty, config);
    config.check_termination = true;
    tests.add("TEST TERMINATION ALL SAME", test_all_same, config);
    tests.add("TEST TERMINATION ALL DIFF", test_all_diff, config);
    tests.add("TEST TERMINATION PROPOSALS", test_proposals, config);
    tests.add("TEST TERMINATION FAULTY", test_faulty, config);

    let test = args.test.as_deref();
    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
