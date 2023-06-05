use assertables::assume;
use env_logger::Builder;
use log::LevelFilter;
use serde::Serialize;
use serde_json::Value;
use std::io::Write;
use sugars::{ rc, refcell };

use dslib::pynode::{ JsonMessage, PyNodeFactory };
use dslib::test::{ TestResult };
use dslib::node::LocalEventType;
use dslib::system::System;

#[derive(Serialize)]
pub struct MessageInit {
    pub value: u64,
}

#[derive(Copy, Clone)]
pub struct TestConfig<'a> {
    pub node_count: u32,
    pub faulty_count: u32,
    pub node_factory: &'a PyNodeFactory,
    pub byz_node_factory: Option<&'a PyNodeFactory>,
    pub seed: u64,
    pub check_termination: bool,
}

pub fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

pub fn get_local_messages(sys: &System<JsonMessage>, node: &str) -> Vec<JsonMessage> {
    sys.get_local_events(node)
        .into_iter()
        .filter(|m| matches!(m.tip, LocalEventType::LocalMessageSend))
        .map(|m| m.msg.unwrap())
        .collect::<Vec<_>>()
}

#[allow(dead_code)]
pub fn build_system(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    let mut node_ids = Vec::new();
    for n in 0..config.node_count {
        node_ids.push(format!("{}", n));
    }
    for node_id in node_ids.iter() {
        let node = config.node_factory.build(
            node_id,
            (node_id, node_ids.clone(), config.faulty_count, config.seed),
            config.seed
        );
        sys.add_node(rc!(refcell!(node)));
    }
    return sys;
}

#[allow(dead_code)]
pub fn build_system_with_byz(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    let mut node_ids = Vec::new();
    for n in 0..config.node_count {
        node_ids.push(format!("{}", n));
    }
    for node_id in node_ids.iter() {
        let node;
        if node_id == "0" {
            node = config.byz_node_factory.unwrap().build(
                node_id,
                (node_id, node_ids.clone(), config.faulty_count, config.seed),
                config.seed
            );
        } else {
            node = config.node_factory.build(
                node_id,
                (node_id, node_ids.clone(), config.faulty_count, config.seed),
                config.seed
            );
        }

        sys.add_node(rc!(refcell!(node)));
    }
    return sys;
}

#[allow(dead_code)]
pub fn send_init_messages(sys: &mut System<JsonMessage>, init_values: &Vec<u64>) {
    for (idx, init_value) in init_values.iter().enumerate() {
        sys.send_local(
            JsonMessage::from("INIT", &(MessageInit { value: *init_value })),
            &format!("{}", idx)
        );
    }
}

#[allow(dead_code)]
pub fn check_consensus(
    sys: &mut System<JsonMessage>,
    nodes: &Vec<String>,
    mut expected_result: Option<u64>
) -> TestResult {
    for node in nodes.iter() {
        let mut messages = get_local_messages(&sys, &node);

        if messages.len() == 0 {
            let res = sys.step_until_local_message(&node);
            assume!(res.is_ok(), format!("Node {}: No messages returned!", node))?;
            messages = get_local_messages(&sys, &node);
        }

        assume!(messages.len() == 1, format!("Node {}: Wrong number of messages!", node))?;
        assume!(messages[0].tip == "RESULT", format!("Node {}: Wrong message type!", node))?;

        let data: Value = serde_json::from_str(&messages[0].data).unwrap();
        let value = data["value"].as_u64().unwrap();
        if expected_result.is_none() {
            expected_result = Some(value);
        }
        assume!(
            value == expected_result.unwrap(),
            format!("Node {}: returned {} instead of {}", node, value, expected_result.unwrap())
        )?;
    }
    Ok(true)
}

#[allow(dead_code)]
pub fn check_delivery(
    sys: &mut System<JsonMessage>,
    msg_type: &str,
    nodes: &Vec<String>,
    mut expected_result: Option<u64>
) -> TestResult {
    for node in nodes.iter() {
        let messages = get_local_messages(&sys, &node);

        assume!(messages.len() > 0, format!("Node {}: No messages returned!", node))?;
        assume!(messages.len() == 1, format!("Node {}: Wrong number of messages!", node))?;
        assume!(messages[0].tip == msg_type, format!("Node {}: Wrong message type!", node))?;

        let data: Value = serde_json::from_str(&messages[0].data).unwrap();
        let value = data["value"].as_u64().unwrap();
        if expected_result.is_none() {
            expected_result = Some(value);
        }
        assume!(
            value == expected_result.unwrap(),
            format!("Node {}: delivered {} instead of {}", node, value, expected_result.unwrap())
        )?;
    }
    Ok(true)
}

#[allow(dead_code)]
pub fn check_not_delivery(sys: &mut System<JsonMessage>, nodes: &Vec<String>) -> TestResult {
    for node in nodes.iter() {
        let messages = get_local_messages(&sys, &node);
        assume!(
            messages.len() == 0,
            format!("Node {}: The message was returned, but it wasn't meant to be!", node)
        )?;
    }
    Ok(true)
}