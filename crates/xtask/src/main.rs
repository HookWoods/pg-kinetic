use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

const COMMANDS: &[&str] = &[
    "check",
    "smoke",
    "smoke-linux",
    "compat",
    "compat-ci",
    "regression",
    "bench-validate",
    "bench-score",
    "docs-check",
    "ci-linux",
];

#[derive(Debug, Default)]
struct Options {
    dry_run: bool,
    list: bool,
    passthrough: Vec<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args().skip(1);
    let Some(command) = arguments.next() else {
        print_usage();
        return Err("a command is required".to_owned());
    };

    if command == "--list" {
        print_commands();
        return Ok(());
    }

    let options = parse_options(arguments);
    let root = workspace_root()?;

    match command.as_str() {
        "check" => run_standard_command(&root, "check", &options),
        "smoke" => run_standard_command(&root, "smoke", &options),
        "smoke-linux" => run_standard_command(&root, "smoke-linux", &options),
        "compat" => run_compat(&root, &options),
        "compat-ci" => run_compat_ci(&root, &options),
        "regression" => run_delegate(
            &root,
            "scripts/regression/run.sh",
            "regression runner",
            &options,
        ),
        "bench-validate" => run_standard_command(&root, "bench-validate", &options),
        "bench-score" => run_bench_score(&root, &options),
        "docs-check" => run_standard_command(&root, "docs-check", &options),
        "ci-linux" => run_standard_command(&root, "ci-linux", &options),
        "--help" | "-h" | "help" => {
            print_usage();
            Ok(())
        }
        _ => Err(format!("unknown command '{command}'")),
    }
}

fn parse_options(arguments: impl Iterator<Item = String>) -> Options {
    let mut options = Options::default();

    for argument in arguments {
        match argument.as_str() {
            "--dry-run" => options.dry_run = true,
            "--list" => options.list = true,
            _ => options.passthrough.push(argument),
        }
    }

    options
}

fn run_standard_command(root: &Path, command: &str, options: &Options) -> Result<(), String> {
    if options.list || !options.passthrough.is_empty() {
        return Err(format!(
            "{command} does not accept additional arguments; use --dry-run where supported"
        ));
    }

    match command {
        "check" => run_command(root, "cargo", vec!["check", "--workspace"], options.dry_run),
        "smoke" => {
            if cfg!(windows) {
                run_command(
                    root,
                    "powershell.exe",
                    vec![
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        "scripts/smoke/psql.ps1",
                    ],
                    options.dry_run,
                )
            } else {
                run_command(root, "bash", vec!["scripts/smoke/psql.sh"], options.dry_run)
            }
        }
        "smoke-linux" => {
            run_command(root, "bash", vec!["scripts/smoke/psql.sh"], options.dry_run)?;
            run_command(
                root,
                "bash",
                vec!["scripts/smoke/read-routing.sh"],
                options.dry_run,
            )?;
            run_command(
                root,
                "bash",
                vec!["scripts/smoke/compat.sh"],
                options.dry_run,
            )?;
            run_command(
                root,
                "bash",
                vec!["scripts/smoke/runtime.sh"],
                options.dry_run,
            )?;
            run_command(
                root,
                "bash",
                vec!["scripts/smoke/mirroring.sh"],
                options.dry_run,
            )?;
            run_command(
                root,
                "bash",
                vec!["scripts/smoke/sharding.sh"],
                options.dry_run,
            )?;
            run_command(
                root,
                "bash",
                vec!["scripts/smoke/performance.sh"],
                options.dry_run,
            )
        }
        "bench-validate" => run_command(
            root,
            "cargo",
            vec![
                "run",
                "-p",
                "pg-kinetic",
                "--",
                "benchmark",
                "validate",
                "--scenario",
                "bench/scenarios/benchmark-simple-query.toml",
            ],
            options.dry_run,
        ),
        "docs-check" => {
            let testing_guide = fs::read_to_string(root.join("docs/testing.md"))
                .map_err(|error| format!("read docs/testing.md: {error}"))?;
            if !testing_guide.contains("## Linux") || !testing_guide.contains("## Windows") {
                return Err("docs/testing.md must document Linux and Windows workflows".to_owned());
            }

            println!("PASS: testing workflow documentation present");
            run_command(
                root,
                "bash",
                vec!["scripts/docs/check-links.sh"],
                options.dry_run,
            )?;
            run_command(
                root,
                npm_command(),
                vec!["--prefix", "docs-site", "run", "check"],
                options.dry_run,
            )
        }
        "ci-linux" => {
            run_standard_command(root, "check", options)?;
            run_standard_command(root, "smoke-linux", options)?;
            run_standard_command(root, "bench-validate", options)?;
            run_standard_command(root, "docs-check", options)
        }
        _ => Err(format!("unsupported standard command '{command}'")),
    }
}

fn run_delegate(
    root: &Path,
    script: &str,
    description: &str,
    options: &Options,
) -> Result<(), String> {
    let mut arguments = vec![script.to_owned()];
    if options.list {
        arguments.push("--list".to_owned());
    }
    arguments.extend(options.passthrough.iter().cloned());
    if options.dry_run {
        arguments.push("--dry-run".to_owned());
    }

    if options.dry_run {
        return run_command(root, "bash", arguments, true);
    }

    if !root.join(script).is_file() {
        println!("SKIP: {description} is not installed");
        return Ok(());
    }

    run_command(root, "bash", arguments, false)
}

fn run_bench_score(root: &Path, options: &Options) -> Result<(), String> {
    if options.list {
        return Err("bench-score does not support --list".to_owned());
    }

    let mut arguments = vec![
        "run".to_owned(),
        "-p".to_owned(),
        "pg-kinetic".to_owned(),
        "--".to_owned(),
        "benchmark".to_owned(),
        "score".to_owned(),
    ];
    let has_baseline = options
        .passthrough
        .iter()
        .any(|argument| argument == "--baseline");
    let has_current = options
        .passthrough
        .iter()
        .any(|argument| argument == "--current");
    let has_format = options
        .passthrough
        .iter()
        .any(|argument| argument == "--format");

    if !has_baseline {
        arguments.push("--baseline".to_owned());
        arguments.push("regression/baselines/performance-score.sample.json".to_owned());
    }
    if !has_current {
        arguments.push("--current".to_owned());
        arguments.push("regression/baselines/performance-score.sample.json".to_owned());
    }

    arguments.extend(options.passthrough.iter().cloned());

    if !has_format {
        arguments.push("--format".to_owned());
        arguments.push("json".to_owned());
    }

    run_command(root, "cargo", arguments, options.dry_run)
}

fn run_compat(root: &Path, options: &Options) -> Result<(), String> {
    let mut arguments = vec![
        "run".to_owned(),
        "-p".to_owned(),
        "pg-kinetic".to_owned(),
        "--".to_owned(),
        "compat".to_owned(),
    ];
    if options.list {
        arguments.push("list".to_owned());
    } else {
        arguments.push("run".to_owned());
    }
    arguments.push("--manifest".to_owned());
    arguments.push("regression/manifest.toml".to_owned());
    arguments.extend(options.passthrough.iter().cloned());

    run_command(root, "cargo", arguments, options.dry_run)
}

fn run_compat_ci(root: &Path, options: &Options) -> Result<(), String> {
    if options.list {
        return Err("compat-ci does not support --list".to_owned());
    }
    let matrix = [
        "rust",
        "go",
        "java",
        "javascript",
        "python",
        "dotnet",
        "c",
        "cpp",
    ];
    for language in matrix {
        run_compat(
            root,
            &Options {
                dry_run: options.dry_run,
                list: false,
                passthrough: vec![
                    "--language".to_owned(),
                    language.to_owned(),
                    "--smoke".to_owned(),
                ],
            },
        )?;
    }
    Ok(())
}

fn run_command<I, S>(root: &Path, program: &str, arguments: I, dry_run: bool) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let arguments = arguments
        .into_iter()
        .map(|argument| argument.as_ref().to_owned())
        .collect::<Vec<_>>();
    let display = std::iter::once(program.to_owned())
        .chain(arguments.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");

    if dry_run {
        println!("DRY-RUN: {display}");
        return Ok(());
    }

    let status = Command::new(program)
        .args(&arguments)
        .current_dir(root)
        .status()
        .map_err(|error| format!("start '{display}': {error}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("'{display}' exited with {status}"))
    }
}

fn npm_command() -> &'static str {
    if cfg!(windows) {
        "npm.cmd"
    } else {
        "npm"
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    let current_dir =
        env::current_dir().map_err(|error| format!("read current directory: {error}"))?;

    current_dir
        .ancestors()
        .find(|directory| {
            fs::read_to_string(directory.join("Cargo.toml"))
                .is_ok_and(|contents| contents.contains("[workspace]"))
        })
        .map(Path::to_path_buf)
        .ok_or_else(|| "could not find a Cargo workspace root".to_owned())
}

fn print_commands() {
    for command in COMMANDS {
        println!("{command}");
    }
}

fn print_usage() {
    println!("usage: cargo run -p xtask -- <command> [--dry-run]");
    println!("commands:");
    print_commands();
    println!("regression passes --list to its Bash runner");
    println!("compat passes filters to the compatibility runner");
    println!("bench-score defaults to the deterministic sample comparison");
}
