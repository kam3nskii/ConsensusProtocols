use clap::Parser;
use log::LevelFilter;
use std::env;

use dslib::pynode::{ PyNodeFactory };
use dslib::test::{ TestResult, TestSuite };

#[path = "../../../utils/utils.rs"]
mod utils;

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    for _ in nodes.iter() {
        init_values.push(sys.gen_range(0..2));
    }

    utils::send_init_messages(&mut sys, &init_values);

    sys.step_until_no_events();

    utils::check_consensus(&mut sys, &nodes, None)
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
    #[clap(long = "nodes", short = 'n', default_value = "6")]
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

    let node_factory = PyNodeFactory::new(&args.impl_path, "BenOrNode");
    let config = utils::TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("TEST SIMPLE", test_simple, config);

    let test = args.test.as_deref();
    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
