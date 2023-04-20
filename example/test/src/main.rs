use assertables::assume;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use std::env;
use std::io::Write;
use sugars::{rc, refcell};

use dslib::node::LocalEventType;
use dslib::pynode::{JsonMessage, PyNodeFactory};
use dslib::system::System;
use dslib::test::{TestResult, TestSuite};

// UTILS -----------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    server_f: &'a PyNodeFactory,
    client_f: &'a PyNodeFactory,
    seed: u64,
    drop_rate: f64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    let client = config
        .client_f
        .build("client", ("client", "server"), config.seed);
    sys.add_node(rc!(refcell!(client)));
    let server = config.server_f.build("server", ("server",), config.seed);
    sys.add_node(rc!(refcell!(server)));
    return sys;
}

fn get_local_messages(sys: &System<JsonMessage>, node: &str) -> Vec<JsonMessage> {
    sys.get_local_events(node)
        .into_iter()
        .filter(|m| matches!(m.tip, LocalEventType::LocalMessageSend))
        .map(|m| m.msg.unwrap())
        .collect::<Vec<_>>()
}

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.set_drop_rate(config.drop_rate);

    let req = JsonMessage::new("REQUEST", r#"{"ans": "?"}"#);
    sys.send_local(req, "client");
    sys.step_until_no_events();

    let messages = get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "Wrong number of messages!")?;
    assume!(messages[0].tip == "RESPONSE", "Wrong message type!")?;
    assume!(
        messages[0].data == r#"{"ans": "42"}"#,
        "Wrong message data!"
    )?;
    Ok(true)
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
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Test to run (optional)
    #[clap(long, short)]
    test: Option<String>,
}

fn main() {
    let args = Args::parse();
    let test = args.test.as_deref();
    init_logger(LevelFilter::Trace);

    env::set_var("PYTHONPATH", format!("{}/python", args.dslib_path));
    let server_f = PyNodeFactory::new(&args.impl_path, "Server");
    let client_f = PyNodeFactory::new(&args.impl_path, "Client");

    let config = TestConfig {
        server_f: &server_f,
        client_f: &client_f,
        seed: args.seed,
        drop_rate: 0.0,
    };

    let mut tests = TestSuite::new();
    if test.is_none() || test.unwrap() == "simple" {
        tests.add("TEST SIMPLE", test_simple, config);
    }

    tests.run();
}
