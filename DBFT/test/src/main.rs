use assertables::assume;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::io::Write;
use sugars::{rc, refcell};

use dslib::node::LocalEventType;
use dslib::pynode::{JsonMessage, PyNodeFactory};
use dslib::system::System;
use dslib::test::{TestResult, TestSuite};

// UTILS -----------------------------------------------------------------------

#[derive(Serialize)]
struct MessageInit {
    value: u64,
}

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    node_count: u32,
    faulty_count: u32,
    node_factory: &'a PyNodeFactory,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    let mut node_ids = Vec::new();
    for n in 0..config.node_count {
        node_ids.push(format!("{}", n));
    }
    for node_id in node_ids.iter() {
        let node = config.node_factory.build(
            node_id,
            (node_id, node_ids.clone(), config.faulty_count),
            config.seed,
        );
        sys.add_node(rc!(refcell!(node)));
    }
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

    sys.send_local(
        JsonMessage::from("INIT", &MessageInit { value: 42 }),
        &format!("{}", 0),
    );

    sys.send_local(
        JsonMessage::from("INIT", &MessageInit { value: 69 }),
        &format!("{}", 1),
    );

    sys.send_local(
        JsonMessage::from("INIT", &MessageInit { value: 0 }),
        &format!("{}", 2),
    );

    sys.send_local(
        JsonMessage::from("INIT", &MessageInit { value: 1 }),
        &format!("{}", 3),
    );

    sys.step_until_no_events();

    // for i in 0..config.node_count {
    //     let node_id = format!("{}", i);
    //     let messages = get_local_messages(&sys, &node_id);

    //     assume!(
    //         messages.len() > 0,
    //         format!("Node {}: No messages returned!", i)
    //     )?;
    //     assume!(
    //         messages.len() == 1,
    //         format!("Node {}: More than 1 message returned!", i)
    //     )?;
    //     assume!(
    //         messages[0].tip == "ACCEPT",
    //         format!("Node {}: Wrong message type!", i)
    //     )?;

    //     let data: Value = serde_json::from_str(&messages[0].data).unwrap();
    //     let value = data["value"].as_u64().unwrap();
    //     assume!(
    //         value == number,
    //         format!("Node {}: returned {} instead of {}", i, value, number)
    //     )?;
    // }

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
    let args = Args::parse();
    let test = args.test.as_deref();
    init_logger(LevelFilter::Trace);

    env::set_var("PYTHONPATH", format!("{}/python", args.dslib_path));
    let node_factory = PyNodeFactory::new(&args.impl_path, "DBFT");
    let config = TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();

    tests.add("TEST SIMPLE", test_simple, config);

    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
