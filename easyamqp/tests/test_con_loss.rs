use std::env;

fn get_env_var_str(var_name: &str, default_value: &str) -> String {
    return match env::var(var_name) {
        Ok(s) => s,
        Err(_) => default_value.to_string(),
    };
}

fn get_env_var_int(var_name: &str, default_value: i32) -> i32 {
    return match env::var(var_name) {
        Ok(s) => s.parse::<i32>().unwrap(),
        Err(_) => default_value,
    };
}


#[test]
#[ignore]
fn integration_connection_loss() {
    println!("I am only used in docker compose based integration tests");
    let rabbit_server = get_env_var_str("RABBIT_SERVER", "127.0.0.1");
    let connection_name = get_env_var_str("CONN_NAME", "playground");
    let count_of_kills = get_env_var_int("KILL_COUNT", 10);
    let expected_conn_count = get_env_var_int("CONN_COUNT", 1);



}
