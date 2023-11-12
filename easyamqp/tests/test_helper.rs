use base64::Engine;
use reqwest::header;

pub async fn list_from_rabbitmqadmin(obj_type: &str) -> Result<String, String> {
    // Replace 'your_username' and 'your_password' with your RabbitMQ username and password
    let username = "guest";
    let password = "guest";

    // Build the URL
    let url = format!("http://localhost:15672/api/{}", obj_type);

    // Create reqwest client with Basic Auth credentials
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // Use this only for testing, remove in production
        .build()
        .map_err(|e| e.to_string())?;

    // Create a request with Basic Auth
    let request = client.get(&url)
        .header(header::AUTHORIZATION, reqwest::header::HeaderValue::from_str(&format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(&format!("{}:{}", username, password))
        )).map_err(|e| e.to_string())?);

    // Send the request asynchronously
    let response = request.send().await.map_err(|e| e.to_string())?;

    // Check if the request was successful (status code 2xx)
    if response.status().is_success() {
        // Return the response body
        let l = response.content_length();
        match response.text().await {
            Ok(body) => Ok(body),
            Err(e) => Err(e.to_string()),
        }
    } else {
        // Return an error message if the request was not successful
        let error_msg = format!("Error: {}", response.status());
        Err(error_msg)
    }
}
