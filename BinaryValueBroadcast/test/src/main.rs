use assertables::assume;
use clap::Parser;
use log::LevelFilter;
use serde_json::Value;
use std::env;

use dslib::pynode::{ JsonMessage, PyNodeFactory };
use dslib::test::{ TestResult, TestSuite };

#[path = "../../../utils/utils.rs"]
mod utils;

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);

    let bin_val: u64 = 1;

    let init_cnt = config.faulty_count + 1;
    for i in 0..init_cnt {
        sys.send_local(
            JsonMessage::from("INIT", &(utils::MessageInit { value: bin_val })),
            &format!("{}", i)
        );
    }

    sys.step_until_no_events();

    for i in 0..config.node_count {
        let node_id = format!("{}", i);
        let messages = utils::get_local_messages(&sys, &node_id);

        assume!(messages.len() > 0, format!("Node {}: No messages returned!", i))?;
        assume!(messages.len() == 1, format!("Node {}: More than 1 message returned!", i))?;
        assume!(messages[0].tip == "DELIVERY", format!("Node {}: Wrong message type!", i))?;

        let data: Value = serde_json::from_str(&messages[0].data).unwrap();
        let value = data["value"].as_u64().unwrap();
        assume!(
            value == bin_val,
            format!("Node {}: returned {} instead of {}", i, value, bin_val)
        )?;
    }

    Ok(true)
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
