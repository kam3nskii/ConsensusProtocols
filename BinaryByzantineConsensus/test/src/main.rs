use assertables::assume;
use clap::Parser;
use log::LevelFilter;
use std::env;
use rand::prelude::*;
use rand_pcg::Pcg64;

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

    if config.check_termination {
        sys.step_until_no_events();
    }

    utils::check_consensus(&mut sys, &nodes, None)
}

fn test_all_one(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), 1);
    utils::send_init_messages(&mut sys, &init_values);

    if config.check_termination {
        sys.step_until_no_events();
    }

    utils::check_consensus(&mut sys, &nodes, Some(1))
}

fn test_all_zero(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), 0);
    utils::send_init_messages(&mut sys, &init_values);

    if config.check_termination {
        sys.step_until_no_events();
    }

    utils::check_consensus(&mut sys, &nodes, Some(0))
}

fn test_half_half(config: &utils::TestConfig) -> TestResult {
    let mut sys = utils::build_system(config);
    let nodes = sys.get_node_ids();

    let mut init_values = Vec::new();
    init_values.resize(nodes.len(), 1);
    let half = nodes.len() / 2;
    for i in 0..half {
        init_values[i] = 0;
    }

    utils::send_init_messages(&mut sys, &init_values);

    if config.check_termination {
        sys.step_until_no_events();
    }

    utils::check_consensus(&mut sys, &nodes, None)
}

fn test_print_stat(config: &utils::TestConfig) -> TestResult {
    let mut percentages = Vec::<u64>::new();
    percentages.push(25);
    percentages.push(50);
    percentages.push(75);

    for seed in 1..=1000 {
        for percentage_of_ones in percentages.iter() {
            let mut sys = utils::build_system_with_custom_seed(config, seed);
            let nodes = sys.get_node_ids();

            sys.set_delays(1.0, 5.0);

            let mut init_values = Vec::new();
            init_values.resize(nodes.len(), 0);

            for i in 0..nodes.len() {
                if (i as f32) / (nodes.len() as f32) > (*percentage_of_ones as f32) / 100.0 {
                    break;
                }
                init_values[i] = 1;
            }

            let mut rand = Pcg64::seed_from_u64(seed);
            init_values.shuffle(&mut rand);

            utils::send_init_messages(&mut sys, &init_values);

            if config.check_termination {
                sys.step_until_no_events();
            }

            assume!(
                utils::check_consensus_and_print_statistics(
                        &mut sys, &nodes, None,
                        config, seed, *percentage_of_ones
                    ).is_ok()
            )?;
        }
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
    env::set_var("PYTHONPATH", "../../dslib/python");
    let args = Args::parse();

    let node_factory_safe = PyNodeFactory::new(&args.impl_path, "SafeBBC");
    let mut config = utils::TestConfig {
        node_count: args.node_count,
        faulty_count: args.faulty_count,
        node_factory: &node_factory_safe,
        byz_node_factory: None,
        seed: args.seed,
        check_termination: false,
    };

    let mut tests = TestSuite::new();
    tests.add("TEST SAFE SIMPLE", test_simple, config);
    tests.add("TEST SAFE ALL ONE", test_all_one, config);
    tests.add("TEST SAFE ALL ZERO", test_all_zero, config);
    tests.add("TEST SAFE HALF/HALF", test_half_half, config);

    let node_factory_psync = PyNodeFactory::new(&args.impl_path, "PsyncBBC");
    config.node_factory = &node_factory_psync;
    config.check_termination = true;
    tests.add("TEST PSYNC SIMPLE", test_simple, config);
    tests.add("TEST PSYNC ALL ONE", test_all_one, config);
    tests.add("TEST PSYNC ALL ZERO", test_all_zero, config);
    tests.add("TEST PSYNC HALF/HALF", test_half_half, config);

    let test = args.test.as_deref();
    if test.is_none() {
        utils::init_logger(LevelFilter::Trace);
        tests.run();
    } else {
        if test.unwrap() == "TEST PSYNC PRINT STAT" {
            utils::init_logger(LevelFilter::Debug);
            config.node_count = 64;
            config.faulty_count = 21;
            tests.add(test.unwrap(), test_print_stat, config);
            tests.run_test(test.unwrap());
        } else {
            utils::init_logger(LevelFilter::Trace);
            tests.run_test(test.unwrap());
        }
    }
}
