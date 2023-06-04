use clap::Parser;
use log::LevelFilter;
use std::env;

use dslib::pynode::{ PyNodeFactory };
use dslib::test::{ TestResult, TestSuite };

#[path = "../../../utils/utils.rs"]
mod utils;

// TESTS -----------------------------------------------------------------------

fn test_all_same(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let value = 42;
    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), value);

    utils::send_init_messages(&mut sys, &init_values);

    utils::check_consensus(&mut sys, &nodes, Some(value))
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
    let config = utils::TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("TEST ALL SAME", test_all_same, config);

    let test = args.test.as_deref();
    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
