use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt;
use sha1::{Digest, Sha1};

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

    // first bucket_index bits match node_id
    // next bit is opposite of node_id (this is what places id in the right bucket)
    // rest of bits are random
    pub fn generate_id_in_bucket(node_id: Id, bucket_index: usize) -> Self {
        // bucket index = number of leading zeros
        let mut id = node_id.id;

        let mut byte = bucket_index / 8;
        let mut bit = bucket_index % 8;
        id[byte] ^= 1 << (7 - bit); // flip node_id bit

        // randomize rest of bits
        let mut rng = rand::thread_rng();
        for i in (bucket_index + 1)..160 {
            byte = i / 8;
            bit = i % 8;

            if rng.gen::<bool>() {
                id[byte] |= 1 << (7 - bit); // set bit to 1
            } else {
                id[byte] &= !(1 << (7 - bit)); // set bit to 0
            }
        }

        Self { id }
    }

    // calculates SHA1 hash of value
    pub fn hash_value(value: Vec<u8>) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(value);
        let hash = hasher.finalize();
        Self {
            id: hash.into()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dist_self_is_zero() {
        let id = Id::generate_id();
        let dist = id.distance(id);
        assert_eq!(dist.id, [0u8; 20]);
    }

    #[test]
    fn test_dist_symmetry() {
        let id1 = Id::generate_id();
        let id2 = Id::generate_id();
        assert_eq!(id1.distance(id2), id2.distance(id1));
    }

    #[test]
    fn test_dist_triangle_inequality() {
        let a = Id::generate_id();
        let b = Id::generate_id();
        let c = Id::generate_id();
        assert!(a.distance(c) <= a.distance(b).distance(b.distance(c)));
    }

    #[test]
    fn test_leading_zeros_all_zero() {
        let id = Id { id: [0u8; 20] };
        assert_eq!(id.leading_zeros(), 160);
    }

    #[test]
    fn test_leading_zeros_all_one() {
        let id = Id { id: [0xFF; 20] };
        assert_eq!(id.leading_zeros(), 0);
    }

    #[test]
    fn test_leading_zeros_known() {
        let mut id = Id { id: [0u8; 20] };
        id.id[1] = 0b00001000; // 4 leading zeros in second byte
        assert_eq!(id.leading_zeros(), 12);
    }

    #[test]
    fn test_generate_id_in_bucket() {
        let node_id = Id::generate_id();
        for i in 0..160 {
            let id = Id::generate_id_in_bucket(node_id, i);
            let dist = node_id.distance(id);
            assert_eq!(dist.leading_zeros() as usize, i);
        }
    }

    #[test]
    fn test_hash_value_deterministic() {
        let value = b"Hello World!".to_vec();
        let hash1 = Id::hash_value(value.clone());
        let hash2 = Id::hash_value(value);
        assert_eq!(hash1, hash2);
    }
}