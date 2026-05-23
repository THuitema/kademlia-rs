use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
// 160-bit ID for nodes and keys stored in the DHT
pub struct Id {
    pub id: [u8; 20],
}

impl Id {
    // distance, bitwise XOR between two IDs
    pub fn distance(&self, other: Id) -> Id {
        let mut result = [0u8; 20];
        for i in 0..20 {
            result[i] = self.id[i] ^ other.id[i];
        }
        Id { id: result }
    }

    // useful for determining the corresponding bucket index for an ID
    pub fn leading_zeros(&self) -> u32 {
        for (i, &byte) in self.id.iter().enumerate() {
            if byte != 0 {
                return (i as u32 * 8) + byte.leading_zeros(); 
            }
        }
        160
    }

    // generate random
    pub fn generate_id() -> Self {
        Self {
            id: rand::thread_rng().gen()
        }
    }
}

impl fmt::Display for Id {
    // print hexadecimal representation of id
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.id {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}