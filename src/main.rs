use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::collections::HashMap;
use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = ConnectionPool::new();
    let mut mqttoptions = MqttOptions::new("test-1", "127.0.0.1", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let sender = String::from("sender");
    let subscriber = String::from("subscribe");
    pool.add_connection(&sender, &mqttoptions).await?;
    pool.add_connection(&subscriber, &mqttoptions).await?;
    let sender_conn = pool.get_connection(&sender).await.unwrap();
    let subscribe_conn = pool.get_connection(&subscriber).await.unwrap().to_owned();
    let send_handle = tokio::spawn(requests(sender_conn));
    let subscribe_handle = tokio::spawn(subscribe(subscribe_conn, &subscriber));
    send_handle.await?;
    println!("send process done");
    pool.remove_connection(&sender).await;
    println!("sender remove");
    pool.remove_connection(&subscriber).await;
    subscribe_conn.client.unsubscribe(&subscriber).await?;
    println!("subscriber unsub");
    pool.remove_connection(&subscriber).await;
    println!("subscriber remove");
    subscribe_handle.await?;
    println!("unsubscribe process done");
    Ok(())
}

async fn subscribe(conn: Arc<MqttConnection>, topic: &String) {
    conn.deref().client
        .subscribe(topic, QoS::AtLeastOnce)
        .await
        .unwrap();
    loop {
        // Waits for and retrieves the next event in the event loop.
        let event = conn.event_loop.poll().await;
        // Performs pattern matching on the retrieved event to determine its type
        match &event {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let topic = publish.topic.clone();
                let message = String::from_utf8_lossy(&publish.payload);
                println!("Received message on {}: {:?}", topic, message);
            }
            Ok(event) => {
                println!("Received event: {:?}", event);
            }
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
        }
    }
}

async fn requests(conn: Arc<MqttConnection>) {
    for i in 1..=10 {
        conn.deref().client
            .publish("/yisa/data", QoS::AtLeastOnce, false, vec![1; i])
            .await
            .unwrap();
        println!("send request");
        time::sleep(Duration::from_secs(1)).await;
    }
    time::sleep(Duration::from_secs(5)).await;
}

struct MqttConnection {
    id: String,
    client: AsyncClient,
    event_loop: EventLoop,
}

impl MqttConnection {
    pub fn new(id: String, client: AsyncClient, event_loop: EventLoop) -> Self {
        MqttConnection {
            id,
            client,
            event_loop,
        }
    }
}

struct ConnectionPool {
    connections: Arc<Mutex<HashMap<String, Arc<MqttConnection>>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        ConnectionPool {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn remove_connection(&self, id: &String) {
        let mut connections = self.connections.deref().lock().await;
        match connections.get(id) {
            Some(conn) => {
                match conn.client.disconnect().await {
                    Ok(_) => {}
                    Err(err) => {
                        println!("{}", err)
                    }
                }
                connections.remove(id);
            }
            None => return,
        }
    }

    pub async fn get_connection(&self, id: &String) -> Option<Arc<MqttConnection>> {
        let connections = self.connections.deref().lock().await;
        connections.get(id).cloned()
    }

    pub async fn add_connection(
        &self,
        id: &String,
        options: &MqttOptions,
    ) -> Result<(), std::io::Error> {
        let mut connections = self.connections.deref().lock().await;
        if connections.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Duplicate connection ID: {}", id),
            ));
        }
        let client = self.create_mqtt_client(options.clone());
        let mqtt_connection = Arc::new(MqttConnection::new(id.clone(), client.0, client.1));
        connections.insert(id.clone(), mqtt_connection);
        Ok(())
    }

    fn create_mqtt_client(&self, options: MqttOptions) -> (AsyncClient, EventLoop) {
        let (client,  event_loop) = AsyncClient::new(options, 10);
        return (client, event_loop);
    }
}
