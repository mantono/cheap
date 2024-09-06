use cheap::channel;
use std::thread;

fn main() {
    let (send0, rec) = channel::<String>(4);

    let send1 = send0.clone();

    thread::spawn(move || {
        // Offering first element to channel
        let element = String::from("Hello ");
        println!("Sending '{}'", element);
        send0.clone().offer(element).unwrap();
    })
    .join()
    .unwrap();

    thread::spawn(move || {
        // Offering second element to channel
        let element = String::from("world!");
        println!("Sending '{}'", element);
        send1.clone().offer(element).unwrap();
    })
    .join()
    .unwrap();

    thread::spawn(move || {
        println!("Received '{}'", rec.poll().unwrap());
        println!("Received '{}'", rec.poll().unwrap());
    })
    .join()
    .unwrap();
}
