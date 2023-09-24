use std::env;

pub fn get_env_var_str(var_name: &str, default_value: &str) -> String {
    return match env::var(var_name) {
        Ok(s) => s,
        Err(_) => default_value.to_string(),
    };
}

pub fn get_env_var_int(var_name: &str, default_value: i32) -> i32 {
    return match env::var(var_name) {
        Ok(s) => s.parse::<i32>().unwrap(),
        Err(_) => default_value,
    };
}
