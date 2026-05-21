use clap::Parser;
use std::net::SocketAddr;
use kademlia_rs::node::KademliaNode;
use kademlia_rs::rpc::send_ping;
use kademlia_rs::id::Id;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    addr: SocketAddr,

    #[arg(short, long)]
    join_addr: Option<SocketAddr>,
}
fn main() {
    let args = Args::parse();

    let node = match KademliaNode::new(args.addr, None, None) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("[main] failed to create node: {e}");
            std::process::exit(1);
        }
    };

    println!("node id: {}", node.id);

    if let Some(join_addr) = args.join_addr {
        println!("sending ping!");
        let nonce = Id::generate_id();
        match send_ping(&node.socket, join_addr, node.id, nonce) {
            Ok(_) => {},
            Err(_) => eprintln!("[main] send_ping() failed")
        }
    }
    node.listen();
}
