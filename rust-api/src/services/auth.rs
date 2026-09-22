use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use crate::error::{AppError, AppResult};

/// Verify password using Argon2 - matches Python's passlib CryptContext with argon2
pub fn verify_password(password: &str, stored_hash: &str) -> AppResult<bool> {
    let argon2 = Argon2::default();
    
    let parsed_hash = PasswordHash::new(stored_hash)
        .map_err(|e| AppError::Auth(format!("Invalid password hash format: {}", e)))?;
    
    match argon2.verify_password(password.as_bytes(), &parsed_hash) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// Hash a password with Argon2id using the same defaults as the web client
/// (`Argon2::default()`), producing a PHC string the login flow verifies. Used by
/// the admin CLI; the web/mobile clients hash before sending.
pub fn hash_password(password: &str) -> AppResult<String> {
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes())
        .map_err(|e| AppError::Auth(format!("Failed to hash password: {}", e)))?;
    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_round_trip() {
        let hash = hash_password("correct horse battery staple").expect("hash");
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password("correct horse battery staple", &hash).unwrap());
        assert!(!verify_password("wrong password", &hash).unwrap());
    }

    #[test]
    fn hashes_are_salted_per_call() {
        let a = hash_password("same password").expect("hash");
        let b = hash_password("same password").expect("hash");
        assert_ne!(a, b);
        assert!(verify_password("same password", &a).unwrap());
        assert!(verify_password("same password", &b).unwrap());
    }
}