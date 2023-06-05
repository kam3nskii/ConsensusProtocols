use assertables::assume;
use clap::Parser;
use log::LevelFilter;
use std::env;

use dslib::pynode::{ PyNodeFactory };
use dslib::test::{ TestResult, TestSuite };

#[path = "../../../utils/utils.rs"]
mod utils;

static DELIVERED: &str = "DELIVERY";

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let bin_value: u64 = 1;

    let mut init_values = Vec::new();
    for _ in nodes.iter() {
        init_values.push(bin_value);
    }

    utils::send_init_messages(&mut sys, &init_values);

    sys.step_until_no_events();

    utils::check_delivery(&mut sys, DELIVERED, &nodes, Some(bin_value))
}

fn test_min_init(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let bin_value: u64 = 1;

    let mut init_values = Vec::new();
    let min_init_nodes_cnt = config.faulty_count + 1;
    for _ in 0..min_init_nodes_cnt {
        init_values.push(bin_value);
    }

    utils::send_init_messages(&mut sys, &init_values);

    sys.step_until_no_events();

    utils::check_delivery(&mut sys, DELIVERED, &nodes, Some(bin_value))
}

fn test_not_enough_init(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let bin_value: u64 = 1;

    let mut init_values = Vec::new();
    let init_nodes_cnt = config.faulty_count;
    for _ in 0..init_nodes_cnt {
        init_values.push(bin_value);
    }

    utils::send_init_messages(&mut sys, &init_values);

    sys.step_until_no_events();

    utils::check_not_delivery(&mut sys, &nodes)
}

fn test_disconnect_after_init(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);

    let bin_value: u64 = 1;

    let mut init_values = Vec::new();
    let min_init_nodes_cnt = config.faulty_count + 1;
    for _ in 0..min_init_nodes_cnt {
        init_values.push(bin_value);
    }

    utils::send_init_messages(&mut sys, &init_values);
    sys.step_for_duration(1.0);

    let mut correct_nodes = Vec::<String>::new();
    let mut disconnected_nodes = Vec::<String>::new();

    for i in 0..config.faulty_count {
        let node = format!("{}", i);
        sys.disconnect_node(&node);
        disconnected_nodes.push(node);
    }
    for i in config.faulty_count..config.node_count {
        let node = format!("{}", i);
        correct_nodes.push(node);
    }

    sys.step_until_no_events();

    assume!(utils::check_not_delivery(&mut sys, &disconnected_nodes).is_ok())?;
    utils::check_delivery(&mut sys, DELIVERED, &correct_nodes, Some(bin_value))
}

fn test_diff_inits(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let bin_value: u64 = 1;

    let mut init_values = Vec::new();
    let init_nodes_cnt = config.faulty_count;
    for _ in 0..init_nodes_cnt {
        init_values.push(bin_value);
    }
    init_values.push((bin_value + 1) % 2);

    utils::send_init_messages(&mut sys, &init_values);

    sys.step_until_no_events();

    utils::check_not_delivery(&mut sys, &nodes)
}

// MAIN ------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
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

    let node_factory = PyNodeFactory::new(&args.impl_path, "BBNode");
    let config = utils::TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        byz_node_factory: None,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("TEST SIMPLE", test_simple, config);
    tests.add("TEST MIN INIT", test_min_init, config);
    tests.add("TEST NOT ENOUGH INIT", test_not_enough_init, config);
    tests.add("TEST DISCONNECT AFTER INIT", test_disconnect_after_init, config);
    tests.add("TEST DIFF INITS", test_diff_inits, config);

    let test = args.test.as_deref();
    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
