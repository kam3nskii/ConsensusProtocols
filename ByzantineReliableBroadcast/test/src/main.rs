use assertables::assume;
use clap::Parser;
use log::LevelFilter;
use std::env;

use dslib::pynode::{ PyNodeFactory };
use dslib::test::{ TestResult, TestSuite };

#[path = "../../../utils/utils.rs"]
mod utils;

static DELIVERED: &str = "ACCEPT";

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let init_value: u64 = 42;

    let mut init_values = Vec::new();
    init_values.push(init_value);
    utils::send_init_messages(&mut sys, &init_values);

    sys.step_until_no_events();

    utils::check_delivery(&mut sys, DELIVERED, &nodes, Some(init_value))
}

fn test_disconnect_after_init(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);

    let init_value: u64 = 42;

    let mut init_values = Vec::new();
    init_values.push(init_value);
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
    utils::check_delivery(&mut sys, DELIVERED, &correct_nodes, Some(init_value))
}


fn test_byzantine(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system_with_byz(config);
    let nodes = sys.get_node_ids();

    let init_value: u64 = 42;

    let mut init_values = Vec::new();
    init_values.push(init_value);
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

    let node_factory = PyNodeFactory::new(&args.impl_path, "RBNode");
    let byz_node_factory = PyNodeFactory::new(&args.impl_path, "ByzRBNode");
    let config = utils::TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        byz_node_factory: Some(&byz_node_factory),
        seed: args.seed,
        check_termination: false,
    };

    let mut tests = TestSuite::new();
    tests.add("TEST SIMPLE", test_simple, config);
    tests.add("TEST DISCONNECT AFTER INIT", test_disconnect_after_init, config);
    tests.add("TEST BYZANTINE", test_byzantine, config);

    let test = args.test.as_deref();
    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
