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

fn send_init_messages(sys: &mut System<JsonMessage>, init_values: &Vec<u64>) {
    for (idx, init_value) in init_values.iter().enumerate() {
        sys.send_local(
            JsonMessage::from("INIT", &MessageInit { value: *init_value }),
            &format!("{}", idx),
        );
    }
}

fn check_consensus(
    sys: &mut System<JsonMessage>,
    nodes: &Vec<String>,
    mut expected_result: Option<u64>,
) -> TestResult {
    for node in nodes.iter() {
        let mut messages = get_local_messages(&sys, &node);

        if messages.len() == 0 {
            let res = sys.step_until_local_message(&node);
            assume!(res.is_ok(), format!("Node {}: No messages returned!", node))?;
            messages = get_local_messages(&sys, &node);
        }

        assume!(
            messages.len() == 1,
            format!("Node {}: Wrong number of messages!", node)
        )?;
        assume!(
            messages[0].tip == "RESULT",
            format!("Node {}: Wrong message type!", node)
        )?;

        let data: Value = serde_json::from_str(&messages[0].data).unwrap();
        let value = data["value"].as_u64().unwrap();
        if expected_result.is_none() {
            expected_result = Some(value);
        }
        assume!(
            value == expected_result.unwrap(),
            format!(
                "Node {}: returned {} instead of {}",
                node,
                value,
                expected_result.unwrap()
            )
        )?;
    }
    Ok(true)
}

// TESTS -----------------------------------------------------------------------

fn test_simple(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    for _ in nodes.iter() {
        init_values.push(sys.gen_range(0..2));
    }

    send_init_messages(&mut sys, &init_values);

    check_consensus(&mut sys, &nodes, None)
}

fn test_all_one(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), 1);
    send_init_messages(&mut sys, &init_values);

    check_consensus(&mut sys, &nodes, Some(1))
}

fn test_all_zero(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), 0);
    send_init_messages(&mut sys, &init_values);

    check_consensus(&mut sys, &nodes, Some(0))
}

fn test_half_half(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), 1);
    let half = nodes.len() / 2;
    for i in 0..half {
        init_values[i] = 0;
    }

    send_init_messages(&mut sys, &init_values);

    check_consensus(&mut sys, &nodes, None)
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
    let node_factory = PyNodeFactory::new(&args.impl_path, "SafeBBC");
    let config = TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();

    tests.add("TEST SIMPLE", test_simple, config);
    tests.add("TEST ALL ONE", test_all_one, config);
    tests.add("TEST ALL ZERO", test_all_zero, config);
    tests.add("TEST HALF/HALF", test_half_half, config);

    if test.is_none() {
        tests.run();
    } else {
        tests.run_test(test.unwrap());
    }
}
