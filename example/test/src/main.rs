use assertables::assume;
use clap::Parser;
use log::LevelFilter;
use std::env;
use sugars::{ rc, refcell };

use dslib::pynode::{ JsonMessage, PyNodeFactory };
use dslib::system::System;
use dslib::test::{ TestResult, TestSuite };

#[path = "../../../utils/utils.rs"]
mod utils;

// UTILS -----------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    server_f: &'a PyNodeFactory,
    client_f: &'a PyNodeFactory,
    seed: u64,
    drop_rate: f64,
}

fn build_system(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    let client = config.client_f.build("client", ("client", "server"), config.seed);
    sys.add_node(rc!(refcell!(client)));
    let server = config.server_f.build("server", ("server",), config.seed);
    sys.add_node(rc!(refcell!(server)));
    return sys;
}

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.set_drop_rate(config.drop_rate);

    let req = JsonMessage::new("REQUEST", r#"{"ans": "?"}"#);
    sys.send_local(req, "client");
    sys.step_until_no_events();

    let messages = utils::get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "Wrong number of messages!")?;
    assume!(messages[0].tip == "RESPONSE", "Wrong message type!")?;
    assume!(messages[0].data == r#"{"ans": "42"}"#, "Wrong message data!")?;
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

    /// Drop rate for messages
    #[clap(long = "drop", short = 'd', default_value = "0.0")]
    drop_rate: f64,

    /// Test to run (optional)
    #[clap(long, short)]
    test: Option<String>,
}

fn main() {
    utils::init_logger(LevelFilter::Trace);
    env::set_var("PYTHONPATH", "../../dslib/python");
    let args = Args::parse();

    let server_f = PyNodeFactory::new(&args.impl_path, "Server");
    let client_f = PyNodeFactory::new(&args.impl_path, "Client");

    let config = TestConfig {
        server_f: &server_f,
        client_f: &client_f,
        seed: args.seed,
        drop_rate: args.drop_rate,
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
