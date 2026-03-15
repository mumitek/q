#[cfg(test)]
#[cfg(feature = "e2e")]
mod e2e_test {
    use qp::{PopRequest, PushRequest, queue_service_client::QueueServiceClient};
    use std::env;
    use tonic::Request;

    #[tokio::test]
    async fn test_push_pop() {
        let addr = env::var("SERVER_URL").expect("Server URL is not provided");

        let mut q_client = QueueServiceClient::connect(addr)
            .await
            .expect("Failed to create client");

        let push_request = Request::new(PushRequest {
            queue_id: "ok".into(),
            message: b"This is the message".into(),
        });
        assert!(
            q_client.push(push_request).await.is_ok(),
            "Expects push to succeed"
        );

        let pop_request = Request::new(PopRequest {
            queue_id: "ok".into(),
        });
        assert!(q_client.pop(pop_request).await.is_ok(), "Expects message");
    }
}
