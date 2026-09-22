//! Admin CLI handled by the `pinepods-api` binary before the server starts.
//!
//! Only `DB_*` environment variables are required (no Valkey/Redis or API URLs),
//! so it works inside the running container:
//!
//! ```text
//! docker exec -i pinepods pinepods-api --create-user \
//!   --username alice --email alice@example.com --password-stdin <<< 'secret'
//! ```
//!
//! Passwords are read from stdin (never from an argument) so they don't end up
//! in shell history or the process list.

use std::io::{self, BufRead};

use crate::config::DatabaseConfig;
use crate::database::DatabasePool;
use crate::error::{AppError, AppResult};
use crate::services::auth::hash_password;

const USAGE: &str = "\
PinePods API admin CLI

Usage:
  pinepods-api --create-user --username <name> --email <email> --password-stdin
               [--fullname <name>] [--admin] [--print-api-key]
  pinepods-api --help

Options:
  --create-user       Create a user and exit
  --username <name>   Username (stored lowercase)
  --email <email>     Email address
  --fullname <name>   Display name (defaults to the username)
  --admin             Grant admin rights (may be used more than once)
  --print-api-key     Also print an API key for the new user
  --password-stdin    Read the password as one line from stdin
  --help              Show this help

The database connection is read from DB_TYPE/DB_HOST/DB_PORT/DB_USER/
DB_PASSWORD/DB_NAME (or a .env file in the working directory).

Examples:
  docker exec -i pinepods pinepods-api --create-user \\
    --username alice --email alice@example.com --password-stdin <<< 'secret'

  printf '%s\\n' \"$ADMIN_PW\" | pinepods-api --create-user \\
    --username admin --email admin@example.com --admin --print-api-key --password-stdin
";

#[derive(Debug, PartialEq)]
pub struct CreateUserArgs {
    pub username: String,
    pub email: String,
    pub fullname: Option<String>,
    pub admin: bool,
    pub print_api_key: bool,
}

#[derive(Debug, PartialEq)]
pub enum CliCommand {
    CreateUser(CreateUserArgs),
    Help,
}

/// Parse CLI arguments (including argv[0]). Returns:
///   * `Ok(None)` when no CLI command is present — the caller should start the server
///   * `Ok(Some(command))` when a command was parsed
///   * `Err(message)` on invalid usage
pub fn parse_args(args: &[String]) -> Result<Option<CliCommand>, String> {
    // Stay out of the way of server startup / `--dump-openapi`: only engage when
    // a CLI command or help was explicitly requested.
    let wants_help = args.iter().any(|a| a == "--help" || a == "-h");
    let wants_create = args.iter().any(|a| a == "--create-user");
    if !wants_help && !wants_create {
        return Ok(None);
    }
    if wants_help {
        return Ok(Some(CliCommand::Help));
    }

    let mut username: Option<String> = None;
    let mut email: Option<String> = None;
    let mut fullname: Option<String> = None;
    let mut admin = false;
    let mut print_api_key = false;
    let mut password_stdin = false;

    let mut i = 1; // skip argv[0]
    while i < args.len() {
        match args[i].as_str() {
            "--create-user" => {}
            "--admin" => admin = true,
            "--print-api-key" => print_api_key = true,
            "--password-stdin" => password_stdin = true,
            "--username" | "--email" | "--fullname" => {
                let flag = args[i].clone();
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| format!("{flag} requires a value"))?
                    .clone();
                match flag.as_str() {
                    "--username" => username = Some(value),
                    "--email" => email = Some(value),
                    _ => fullname = Some(value),
                }
            }
            other => return Err(format!("Unknown argument: {other}")),
        }
        i += 1;
    }

    let username = username.ok_or("Missing --username")?;
    let email = email.ok_or("Missing --email")?;
    if username.trim().is_empty() {
        return Err("--username cannot be empty".to_string());
    }
    if email.trim().is_empty() {
        return Err("--email cannot be empty".to_string());
    }
    if !password_stdin {
        return Err("Missing --password-stdin (passwords are read from stdin)".to_string());
    }

    Ok(Some(CliCommand::CreateUser(CreateUserArgs {
        username,
        email,
        fullname,
        admin,
        print_api_key,
    })))
}

/// Run a CLI command when one was requested. Returns true when the process
/// handled a command and should exit without starting the server.
pub async fn maybe_run(args: &[String]) -> AppResult<bool> {
    let command = match parse_args(args) {
        Ok(Some(command)) => command,
        Ok(None) => return Ok(false),
        Err(message) => {
            eprintln!("Error: {message}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    match command {
        CliCommand::Help => {
            println!("{USAGE}");
            Ok(true)
        }
        CliCommand::CreateUser(args) => {
            match create_user(&args).await {
                Ok(()) => Ok(true),
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

async fn create_user(args: &CreateUserArgs) -> AppResult<()> {
    let db_config = DatabaseConfig::from_env()?;
    let db = DatabasePool::from_database_config(&db_config).await?;

    let username = args.username.trim().to_lowercase();
    let email = args.email.trim().to_string();
    let fullname = args
        .fullname
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&username)
        .to_string();

    let password = read_password_stdin()?;

    // `add_admin_user` does `ON CONFLICT DO NOTHING` and then returns the
    // existing row, which would silently "succeed" on a duplicate username.
    // Check first so both paths fail loudly instead.
    if db.get_user_id_from_username(&username).await.is_ok() {
        return Err(AppError::Config(format!(
            "Username '{username}' is already taken"
        )));
    }

    let hashed_pw = hash_password(&password)?;
    let user_id = if args.admin {
        db.add_admin_user(&fullname, &username, &email, &hashed_pw).await?
    } else {
        db.add_user(&fullname, &username, &email, &hashed_pw).await?
    };
    db.ensure_user_stats(user_id).await?;

    let role = if args.admin { "admin" } else { "user" };
    println!("Created {role} '{username}' (id {user_id})");

    if args.print_api_key {
        let api_key = db.create_or_get_api_key(user_id).await?;
        println!("API key: {api_key}");
    }

    Ok(())
}

fn read_password_stdin() -> AppResult<String> {
    let mut line = String::new();
    io::stdin()
        .lock()
        .read_line(&mut line)
        .map_err(|e| AppError::Config(format!("Failed to read password from stdin: {e}")))?;

    let password = line.trim_end_matches(|c| c == '\n' || c == '\r');
    if password.is_empty() {
        return Err(AppError::Config(
            "No password received on stdin (use --password-stdin)".to_string(),
        ));
    }
    Ok(password.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        std::iter::once("pinepods-api".to_string())
            .chain(values.iter().map(|v| v.to_string()))
            .collect()
    }

    #[test]
    fn no_cli_flags_means_server_start() {
        assert_eq!(parse_args(&args(&[])).unwrap(), None);
        // Server flags (like --dump-openapi) must not be treated as CLI errors.
        assert_eq!(
            parse_args(&args(&["--dump-openapi", "openapi.json"])).unwrap(),
            None
        );
    }

    #[test]
    fn parses_create_user() {
        let parsed = parse_args(&args(&[
            "--create-user",
            "--username",
            "Alice",
            "--email",
            "alice@example.com",
            "--fullname",
            "Alice A.",
            "--admin",
            "--print-api-key",
            "--password-stdin",
        ]))
        .unwrap();

        match parsed {
            Some(CliCommand::CreateUser(args)) => {
                assert_eq!(args.username, "Alice");
                assert_eq!(args.email, "alice@example.com");
                assert_eq!(args.fullname.as_deref(), Some("Alice A."));
                assert!(args.admin);
                assert!(args.print_api_key);
            }
            other => panic!("unexpected parse result: {other:?}"),
        }
    }

    #[test]
    fn help_wins() {
        assert_eq!(
            parse_args(&args(&["--help"])).unwrap(),
            Some(CliCommand::Help)
        );
    }

    #[test]
    fn rejects_missing_and_unknown_arguments() {
        assert!(parse_args(&args(&["--create-user", "--email", "a@b.c", "--password-stdin"]))
            .unwrap_err()
            .contains("--username"));
        assert!(parse_args(&args(&[
            "--create-user",
            "--username",
            "a",
            "--email",
            "a@b.c",
            "--password-stdin",
            "--bogus"
        ]))
        .unwrap_err()
        .contains("Unknown argument"));
        assert!(parse_args(&args(&[
            "--create-user",
            "--username",
            "a",
            "--email",
            "a@b.c"
        ]))
        .unwrap_err()
        .contains("--password-stdin"));
        assert!(parse_args(&args(&["--create-user", "--username"])).is_err());
    }
}
